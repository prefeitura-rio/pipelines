# -*- coding: utf-8 -*-
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union

import geopandas as gpd
import numpy as np
import pandas as pd
from prefect import task
import requests

from pipelines.rj_escritorio.flooding_detection.utils import (
    download_file,
    h3_id_to_polygon,
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
    predictions_buffer_key: str,
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
    # TODO: Implement
    raise NotImplementedError()


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
    # TODO: Implement
    raise NotImplementedError()


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
    df_cameras = gpd.read_file(
        cameras_data_path, GEOM_POSSIBLE_NAMES="geometry", KEEP_GEOM_COLUMNS="NO"
    )
    log("Successfully downloaded cameras data.")
    log(f"Cameras shape: {df_cameras.shape}")

    # Get rain data
    rain_data = requests.get(rain_api_data_url).json()
    df_rain = pd.DataFrame(rain_data)
    df_rain["last_update"] = last_update
    df_rain = df_rain.rename(columns={"status": "status_chuva"})
    geometry = df_rain["id_h3"].apply(lambda h3_id: h3_id_to_polygon(h3_id))
    df_rain_geo = gpd.GeoDataFrame(df_rain, geometry=geometry)
    df_rain_geo.crs = {"init": "epsg:4326"}
    log("Successfully downloaded rain data.")
    log(f"Rain data shape: {df_rain.shape}")

    # Join the dataframes
    df_cameras_h3: gpd.GeoDataFrame = gpd.sjoin(
        df_cameras, df_rain_geo, how="left", op="within"
    )
    df_cameras_h3 = df_cameras_h3.drop(columns=["index_right"])
    df_cameras_h3 = df_cameras_h3[df_cameras_h3["id_h3"].notnull()]
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
