# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Any, Dict, List, Union

from prefect import task


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
    # TODO: Implement
    raise NotImplementedError()


@task
def get_openai_api_key() -> str:
    """
    Gets the OpenAI API key.

    Returns:
        The OpenAI API key.
    """
    # TODO: Implement
    raise NotImplementedError()


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
def get_raining_hexagons(
    rain_api_data_url: str,
) -> List[str]:
    """
    Gets the raining hexagons from the rain API.

    Args:
        rain_api_data_url: The rain API data url.

    Returns:
        The raining hexagons.
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
    hexagons: List[str],
    last_update: datetime,
    predictions_buffer_key: str,
) -> List[Dict[str, Union[str, float]]]:
    """
    Picks cameras based on the raining hexagons and last update.

    Args:
        hexagons: The H3 hexagons that are raining.
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
    # TODO: Implement
    raise NotImplementedError()


@task
def update_flooding_api_data(
    prediction: List[Dict[str, Union[str, float, bool]]],
    cameras: List[Dict[str, Union[str, float]]],
    images: List[str],
    data_key: str,
    last_update_key: str,
) -> None:
    """
    Updates Redis keys with flooding detection data and last update datetime (now).

    Args:
        prediction: The AI predictions in the following format:
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
