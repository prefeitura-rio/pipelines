# -*- coding: utf-8 -*-
# TODO: Make it resilient to camera failures
import asyncio
from datetime import datetime
import json
from pathlib import Path
import random
from typing import Dict, List, Tuple, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import pendulum
from prefect import task
import requests
from shapely.geometry import Point

from pipelines.rj_escritorio.flooding_detection.utils import (
    capture_snapshots_async,
    download_file,
    make_requests_async,
    redis_add_to_prediction_buffer,
    redis_get_prediction_buffer,
)
from pipelines.utils.utils import get_redis_client, get_vault_secret, log


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


@task(nout=2)
def get_predictions(
    images: List[str],
    flooding_prompt: str,
    openai_api_key: str,
    openai_api_model: str,
    openai_api_max_tokens: int = 300,
    openai_api_url: str = "https://api.openai.com/v1/chat/completions",
) -> Tuple[List[bool], List[Dict[str, Union[str, float, bool]]]]:
    """
    Gets the flooding detection prediction from OpenAI API.

    Args:
        image: The image in base64 format.
        flooding_prompt: The flooding prompt.
        openai_api_key: The OpenAI API key.
        openai_api_model: The OpenAI API model.
        openai_api_max_tokens: The OpenAI API max tokens.
        openai_api_url: The OpenAI API URL.

    Returns:
        A mask of success and the predictions in the following format:
            [
                {
                    "object": "alagamento",
                    "label": True,
                    "confidence": 0.7,
                },
                ...
            ]
    """
    # TODO:
    # - Add confidence value
    # Setup the request
    methods = ["POST"] * len(images)
    headers = [
        {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {openai_api_key}",
        }
    ] * len(images)
    urls = [openai_api_url] * len(images)
    payloads = [
        {
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
        for image in images
    ]
    success_mask, responses = asyncio.run(
        make_requests_async(methods, urls, headers, payloads)
    )
    results = []
    results_success_mask = []
    for success, response in zip(success_mask, responses):
        if not success:
            results.append(None)
            results_success_mask.append(False)
        data: dict = response.json()
        if data.get("error"):
            results.append(None)
            results_success_mask.append(False)
        content: str = data["choices"][0]["message"]["content"]
        json_string = content.replace("```json\n", "").replace("\n```", "")
        json_object = json.loads(json_string)
        flooding_detected = json_object["flooding_detected"]
        results.append(
            {
                "object": "alagamento",
                "label": flooding_detected,
                "confidence": 0.7,
            }
        )
        results_success_mask.append(True)
    log(f"Successfully got predictions: {results}")
    return results_success_mask, results


@task
def get_snapshots(
    cameras: List[Dict[str, Union[str, float]]],
) -> List[str]:
    """
    Gets a snapshot from a camera.

    Args:
        cameras: A list of cameras in the following format:
            {
                "id_camera": "1",
                "url_camera": "rtsp://...",
                "latitude": -22.912,
                "longitude": -43.230,
            }

    Returns:
        The snapshots in base64 format.
    """
    urls = [camera["url_camera"] for camera in cameras]
    snapshots = asyncio.run(capture_snapshots_async(urls))
    return snapshots


@task
def pick_cameras(
    rain_api_data_url: str,
    cameras_data_url: str,
    last_update: datetime,
    predictions_buffer_key: str,
    number_mock_rain_cameras: int = 0,
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
    # Download the cameras data
    cameras_data_path = Path("/tmp") / "cameras_geo_min.csv"
    if not download_file(url=cameras_data_url, output_path=cameras_data_path):
        raise RuntimeError("Failed to download the cameras data.")
    cameras = pd.read_csv(cameras_data_path)
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

    # Modify status based on buffers
    for _, row in df_cameras_h3.iterrows():
        predictions_buffer_camera_key = f"{predictions_buffer_key}_{row['id_camera']}"
        predictions_buffer = redis_get_prediction_buffer(predictions_buffer_camera_key)
        # Get most common prediction
        most_common_prediction = max(
            set(predictions_buffer), key=predictions_buffer.count
        )
        # Add classifications
        if most_common_prediction or predictions_buffer[-1]:
            row["status"] = "chuva moderada"

    # Mock a few cameras when argument is set
    if number_mock_rain_cameras > 0:
        df_len = len(df_cameras_h3)
        for _ in range(number_mock_rain_cameras):
            mocked_index = random.randint(0, df_len)
            df_cameras_h3.loc[mocked_index, "status"] = "chuva moderada"
            log(f'Mocked camera ID: {df_cameras_h3.loc[mocked_index]["id_camera"]}')

    # Pick cameras
    mask = np.logical_not(df_cameras_h3["status"].isin(["sem chuva", "chuva fraca"]))
    df_cameras_h3 = df_cameras_h3[mask]
    log("Successfully picked cameras.")
    log(f"Picked cameras shape: {df_cameras_h3.shape}")

    # Set output
    output = []
    for _, row in df_cameras_h3.iterrows():
        output.append(
            {
                "id_camera": row["id_camera"],
                "nome_camera": row["nome"],
                "url_camera": row["rtsp"],
                "latitude": row["geometry"].y,
                "longitude": row["geometry"].x,
            }
        )
    log(f"Picked cameras: {output}")
    return output


@task
def update_flooding_api_data(
    predictions: List[Dict[str, Union[str, float, bool]]],
    predictions_success_mask: List[bool],
    cameras: List[Dict[str, Union[str, float]]],
    images: List[str],
    data_key: str,
    last_update_key: str,
    predictions_buffer_key: str,
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
        predictions_buffer_key: The Redis key for the predictions buffer.
    """
    # Build API data
    last_update = pendulum.now(tz="America/Sao_Paulo")
    api_data = []
    for prediction, success, camera, image in zip(
        predictions, predictions_success_mask, cameras, images
    ):
        if not success:
            continue
        # Get AI classifications
        ai_classification = []
        current_prediction = prediction["label"]
        predictions_buffer_camera_key = (
            f"{predictions_buffer_key}_{camera['id_camera']}"
        )
        predictions_buffer = redis_add_to_prediction_buffer(
            predictions_buffer_camera_key, current_prediction
        )
        # Get most common prediction
        most_common_prediction = max(
            set(predictions_buffer), key=predictions_buffer.count
        )
        # Add classifications
        if most_common_prediction:
            ai_classification.append(
                {
                    "object": "alagamento",
                    "label": True,
                    "confidence": 0.7,
                }
            )
            api_data.append(
                {
                    "datetime": last_update.to_datetime_string(),
                    "id_camera": camera["id_camera"],
                    "url_camera": camera["url_camera"],
                    "latitude": camera["latitude"],
                    "longitude": camera["longitude"],
                    "image_base64": image,
                    "ai_classification": ai_classification,
                }
            )

    # Update API data
    redis_client = get_redis_client(db=1)
    redis_client.set(data_key, api_data)
    redis_client.set(last_update_key, last_update.to_datetime_string())
    log("Successfully updated flooding detection data.")
