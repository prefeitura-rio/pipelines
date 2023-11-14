# -*- coding: utf-8 -*-
import base64
from datetime import datetime
import io
import json
from pathlib import Path
from typing import Dict, List, Union

import cv2
import geopandas as gpd
import numpy as np
import pandas as pd
from PIL import Image
from prefect import task
import requests
from shapely.geometry import Point

from pipelines.rj_escritorio.flooding_detection.utils import (
    download_file,
)
from pipelines.utils.utils import get_vault_secret, log


@task
def get_last_update(
    rain_api_update_url: str,
) -> datetime:
    """
    Gets the last update datetime from the rain API.

    Args:
        rain_api_update_url: The rain API update url.

    Returns:
        The last update datetime.
    """
    data = requests.get(rain_api_update_url).text
    data = data.strip('"')
    log(f"Last update: {data}")
    return datetime.strptime(data, "%d/%m/%Y %H:%M:%S")


@task
def get_openai_api_key(secret_path: str) -> str:
    """
    Gets the OpenAI API key.

    Args:
        secret_path: The secret path.

    Returns:
        The OpenAI API key.
    """
    secret = get_vault_secret(secret_path)["data"]
    return secret["api_key"]


@task
def get_prediction(
    image: str,
    flooding_prompt: str,
    openai_api_key: str,
    openai_api_model: str,
    predictions_buffer_key: str,
    openai_api_max_tokens: int = 300,
    openai_api_url: str = "https://api.openai.com/v1/chat/completions",
) -> Dict[str, Union[str, float, bool]]:
    """
    Gets the flooding detection prediction from OpenAI API.

    Args:
        image: The image in base64 format.
        flooding_prompt: The flooding prompt.
        openai_api_key: The OpenAI API key.
        predictions_buffer_key: The Redis key for the predictions buffer.

    Returns:
        The prediction in the following format:
            {
                "object": "alagamento",
                "label": True,
                "confidence": 0.7,
            }
    """
    # TODO:
    # - Add confidence value
    # Setup the request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {openai_api_key}",
    }
    payload = {
        "model": openai_api_model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": flooding_prompt,
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{image}"},
                    },
                ],
            }
        ],
        "max_tokens": openai_api_max_tokens,
    }
    response = requests.post(openai_api_url, headers=headers, json=payload)
    data: dict = response.json()
    if data.get("error"):
        raise RuntimeError(f"Failed to get prediction: {data['error']}")
    content: str = data["choices"][0]["message"]["content"]
    json_string = content.replace("```json\n", "").replace("\n```", "")
    json_object = json.loads(json_string)
    flooding_detected = json_object["flooding_detected"]
    return {
        "object": "alagamento",
        "label": flooding_detected,
        "confidence": 0.7,
    }


@task
def get_snapshot(
    camera: Dict[str, Union[str, float]],
) -> str:
    """
    Gets a snapshot from a camera.

    Args:
        camera: The camera in the following format:
            {
                "id_camera": "1",
                "url_camera": "rtsp://...",
                "latitude": -22.912,
                "longitude": -43.230,
            }

    Returns:
        The snapshot in base64 format.
    """
    rtsp_url = camera["url_camera"]
    cap = cv2.VideoCapture(rtsp_url)
    ret, frame = cap.read()
    if not ret:
        raise RuntimeError(f"Failed to get snapshot from URL {rtsp_url}.")
    cap.release()
    img = Image.fromarray(frame)
    buffer = io.BytesIO()
    img.save(buffer, format="JPEG")
    img_b64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
    log(f"Successfully got snapshot from URL {rtsp_url}.")
    return img_b64


@task
def pick_cameras(
    rain_api_data_url: str,
    cameras_data_url: str,
    last_update: datetime,
    predictions_buffer_key: str,
) -> List[Dict[str, Union[str, float]]]:
    """
    Picks cameras based on the raining hexagons and last update.

    Args:
        rain_api_data_url: The rain API data url.
        last_update: The last update datetime.
        predictions_buffer_key: The Redis key for the predictions buffer.

    Returns:
        A list of cameras in the following format:
            [
                {
                    "id_camera": "1",
                    "url_camera": "rtsp://...",
                    "latitude": -22.912,
                    "longitude": -43.230,
                },
                ...
            ]
    """
    # TODO:
    # - Must always pick cameras whose buffer contains flooding predictions
    # Download the cameras data
    cameras_data_path = Path("/tmp") / "cameras_geo_min.csv"
    if not download_file(url=cameras_data_url, output_path=cameras_data_path):
        raise RuntimeError("Failed to download the cameras data.")
    cameras = pd.read_csv("cameras.csv")
    cameras = cameras.drop(columns=["geometry"])
    geometry = [Point(xy) for xy in zip(cameras["longitude"], cameras["latitude"])]
    df_cameras = gpd.GeoDataFrame(cameras, geometry=geometry)
    df_cameras.crs = {"init": "epsg:4326"}
    log("Successfully downloaded cameras data.")
    log(f"Cameras shape: {df_cameras.shape}")

    # Get rain data
    rain_data = requests.get(rain_api_data_url).json()
    df_rain = pd.DataFrame(rain_data)
    df_rain["last_update"] = last_update
    log("Successfully downloaded rain data.")
    log(f"Rain data shape: {df_rain.shape}")

    # Join the dataframes
    df_cameras_h3 = pd.merge(df_cameras, df_rain, how="left", on="id_h3")
    log("Successfully joined the dataframes.")
    log(f"Cameras H3 shape: {df_cameras_h3.shape}")

    # Pick cameras
    mask = np.logical_not(
        df_cameras_h3["status_chuva"].isin(["sem chuva", "chuva fraca"])
    )
    df_cameras_h3 = df_cameras_h3[mask]
    log("Successfully picked cameras.")
    log(f"Picked cameras shape: {df_cameras_h3.shape}")

    # Set output
    output = []
    for _, row in df_cameras_h3.iterrows():
        output.append(
            {
                "id_camera": row["codigo"],
                "url_camera": row["nome_da_camera"],
                "latitude": row["geometry"].y,
                "longitude": row["geometry"].x,
            }
        )
    log(f"Picked cameras: {output}")
    return output


@task
def update_flooding_api_data(
    predictions: List[Dict[str, Union[str, float, bool]]],
    cameras: List[Dict[str, Union[str, float]]],
    images: List[str],
    data_key: str,
    last_update_key: str,
) -> None:
    """
    Updates Redis keys with flooding detection data and last update datetime (now).

    Args:
        predictions: The AI predictions in the following format:
            [
                {
                    "object": "alagamento",
                    "label": True,
                    "confidence": 0.7,
                },
                ...
            ]
        cameras: A list of cameras in the following format:
            [
                {
                    "id_camera": "1",
                    "url_camera": "rtsp://...",
                    "latitude": -22.912,
                    "longitude": -43.230,
                },
                ...
            ]
        images: A list of images in base64 format.
        data_key: The Redis key for the flooding detection data.
        last_update_key: The Redis key for the last update datetime.
    """
    # TODO: Implement
    raise NotImplementedError()
