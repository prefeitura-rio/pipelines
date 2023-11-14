# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Union

import h3
import requests
from shapely.geometry import Polygon


def download_file(url: str, output_path: Union[str, Path]) -> bool:
    """
    Downloads a file from a URL.

    Args:
        url: The URL.
        output_path: The output path.

    Returns:
        Whether the file was downloaded successfully.
    """
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        return True
    return False


def h3_id_to_polygon(h3_id: str):
    """
    Converts an H3 ID to a Polygon.

    Args:
        h3_id: The H3 ID.

    Returns:
        The Polygon.
    """
    return Polygon(h3.h3_to_geo_boundary(h3_id, geo_json=True))
