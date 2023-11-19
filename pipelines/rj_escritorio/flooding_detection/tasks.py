# -*- coding: utf-8 -*-
import base64
from datetime import datetime, timedelta
import io
import json
from pathlib import Path
import random
from typing import Dict, List, Tuple, Union

import cv2
import geopandas as gpd
import numpy as np
import pandas as pd
import pendulum
from PIL import Image
from prefect import task
import requests
from shapely.geometry import Point

from pipelines.rj_escritorio.flooding_detection.utils import (
    download_file,
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


@task
def get_prediction(
    camera_with_image: Dict[str, Union[str, float]],
    flooding_prompt: str,
    openai_api_key: str,
    openai_api_model: str,
    openai_api_max_tokens: int = 300,
    openai_api_url: str = "https://api.openai.com/v1/chat/completions",
) -> Dict[str, Union[str, float, bool]]:
    """
    Gets the flooding detection prediction from OpenAI API.

    Args:
        camera_with_image: The camera with image in the following format:
            {
                "id_camera": "1",
                "url_camera": "rtsp://...",
                "latitude": -22.912,
                "longitude": -43.230,
                "image_base64": "base64...",
                "attempt_classification": True,
            }
        flooding_prompt: The flooding prompt.
        openai_api_key: The OpenAI API key.
        openai_api_model: The OpenAI API model.
        openai_api_max_tokens: The OpenAI API max tokens.
        openai_api_url: The OpenAI API URL.

    Returns: The camera with image and classification in the following format:
        {
            "id_camera": "1",
            "url_camera": "rtsp://...",
            "latitude": -22.912,
            "longitude": -43.230,
            "image_base64": "base64...",
            "ai_classification": [
                {
                    "object": "alagamento",
                    "label": True,
                    "confidence": 0.7,
                }
            ],
        }
    """
    # TODO:
    # - Add confidence value
    # Setup the request
    if not camera_with_image["attempt_classification"]:
        camera_with_image["ai_classification"] = [
            {
                "object": "alagamento",
                "label": False,
                "confidence": 0.7,
            }
        ]
        return camera_with_image
    if not camera_with_image["image_base64"]:
        camera_with_image["ai_classification"] = [
            {
                "object": "alagamento",
                "label": None,
                "confidence": 0.7,
            }
        ]
        return camera_with_image
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
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{camera_with_image['image_base64']}"
                        },
                    },
                ],
            }
        ],
        "max_tokens": openai_api_max_tokens,
    }
    response = requests.post(openai_api_url, headers=headers, json=payload)
    data: dict = response.json()
    if data.get("error"):
        flooding_detected = None
        log(f"Failed to get prediction: {data['error']}")
    else:
        content: str = data["choices"][0]["message"]["content"]
        json_string = content.replace("```json\n", "").replace("\n```", "")
        json_object = json.loads(json_string)
        flooding_detected = json_object["flooding_detected"]
        log(f"Successfully got prediction: {flooding_detected}")
    camera_with_image["ai_classification"] = [
        {
            "object": "alagamento",
            "label": flooding_detected,
            "confidence": 0.7,
        }
    ]
    return camera_with_image


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=1),
)
def get_snapshot(
    camera: Dict[str, Union[str, float]],
) -> Dict[str, Union[str, float]]:
    """
    Gets a snapshot from a camera.

    Args:
        camera: The camera in the following format:
            {
                "id_camera": "1",
                "url_camera": "rtsp://...",
                "latitude": -22.912,
                "longitude": -43.230,
                "attempt_classification": True,
            }

    Returns:
        The camera with image in the following format:
            {
                "id_camera": "1",
                "url_camera": "rtsp://...",
                "latitude": -22.912,
                "longitude": -43.230,
                "attempt_classification": True,
                "image_base64": "base64...",
            }
    """
    try:
        rtsp_url = camera["url_camera"]
        cap = cv2.VideoCapture(rtsp_url)
        ret, frame = cap.read()
        if not ret:
            raise RuntimeError(f"Failed to get snapshot from URL {rtsp_url}.")
        cap.release()
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        img = Image.fromarray(frame)
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG")
        img_b64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
        log(f"Successfully got snapshot from URL {rtsp_url}.")
        camera["image_base64"] = img_b64
    except Exception:
        log(f"Failed to get snapshot from URL {rtsp_url}.")
        camera["image_base64"] = None
    return camera


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
                    "attempt_classification": True,
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
        # Get last prediction
        last_prediction = predictions_buffer[-1]
        # Add classifications
        if most_common_prediction or last_prediction:
            row["status"] = "chuva moderada"

    # Mock a few cameras when argument is set
    if number_mock_rain_cameras > 0:
        df_len = len(df_cameras_h3)
        for _ in range(number_mock_rain_cameras):
            mocked_index = random.randint(0, df_len)
            df_cameras_h3.loc[mocked_index, "status"] = "chuva moderada"
            log(f'Mocked camera ID: {df_cameras_h3.loc[mocked_index]["id_camera"]}')

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
                "attempt_classification": (
                    row["status"] not in ["sem chuva", "chuva fraca"]
                ),
            }
        )
    log(f"Picked cameras: {output}")
    return output


@task
def update_flooding_api_data(
    cameras_with_image_and_classification: List[Dict[str, Union[str, float, bool]]],
    data_key: str,
    last_update_key: str,
    predictions_buffer_key: str,
) -> None:
    """
    Updates Redis keys with flooding detection data and last update datetime (now).

    Args:
        cameras_with_image_and_classification: The cameras with image and classification
            in the following format:
                [
                    {
                        "id_camera": "1",
                        "url_camera": "rtsp://...",
                        "latitude": -22.912,
                        "longitude": -43.230,
                        "image_base64": "base64...",
                        "ai_classification": [
                            {
                                "object": "alagamento",
                                "label": True,
                                "confidence": 0.7,
                            }
                        ],
                    },
                    ...
                ]
        data_key: The Redis key for the flooding detection data.
        last_update_key: The Redis key for the last update datetime.
        predictions_buffer_key: The Redis key for the predictions buffer.
    """
    # Build API data
    last_update = pendulum.now(tz="America/Sao_Paulo")
    api_data = []
    for camera_with_image_and_classification in cameras_with_image_and_classification:
        # Get AI classifications
        ai_classification = []
        current_prediction = camera_with_image_and_classification["ai_classification"][
            0
        ]["label"]
        if current_prediction is None:
            api_data.append(
                {
                    "datetime": last_update.to_datetime_string(),
                    "id_camera": camera_with_image_and_classification["id_camera"],
                    "url_camera": camera_with_image_and_classification["url_camera"],
                    "latitude": camera_with_image_and_classification["latitude"],
                    "longitude": camera_with_image_and_classification["longitude"],
                    "image_base64": camera_with_image_and_classification[
                        "image_base64"
                    ],
                    "ai_classification": ai_classification,
                }
            )
            continue
        predictions_buffer_camera_key = f"{predictions_buffer_key}_{camera_with_image_and_classification['id_camera']}"  # noqa
        predictions_buffer = redis_add_to_prediction_buffer(
            predictions_buffer_camera_key, current_prediction
        )
        # Get most common prediction
        most_common_prediction = max(
            set(predictions_buffer), key=predictions_buffer.count
        )
        # Add classifications
        ai_classification.append(
            {
                "object": "alagamento",
                "label": most_common_prediction,
                "confidence": 0.7,
            }
        )
        api_data.append(
            {
                "datetime": last_update.to_datetime_string(),
                "id_camera": camera_with_image_and_classification["id_camera"],
                "url_camera": camera_with_image_and_classification["url_camera"],
                "latitude": camera_with_image_and_classification["latitude"],
                "longitude": camera_with_image_and_classification["longitude"],
                "image_base64": camera_with_image_and_classification["image_base64"],
                "ai_classification": ai_classification,
            }
        )

    # Update API data
    redis_client = get_redis_client(db=1)
    redis_client.set(data_key, api_data)
    redis_client.set(last_update_key, last_update.to_datetime_string())
    log("Successfully updated flooding detection data.")
