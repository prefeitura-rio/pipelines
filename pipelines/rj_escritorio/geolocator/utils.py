# -*- coding: utf-8 -*-
# flake8: noqa: E722
"""
General purpose functions for the geolocator project
"""

import requests

from geobr import read_municipality
from shapely.geometry import Point


def geolocator(query_string: str) -> list:
    """
    Utiliza a api do waze pra geolocalizar (lat/long) um endereço.
    """

    url = (
        f"https://gapi.waze.com/autocomplete/q?q={query_string}&e=ALL&c=web"
        "&gxy=1&exp=8%2C10%2C12&v=-22.9370135%2C-43.18795323%3B-22.9301167%2C-43.1815052&lang=pt"
    )

    payload = {}
    headers = {}
    try:
        response = requests.request("GET", url, headers=headers, data=payload)

        long = response.json()[1][0][3]["x"]
        lat = response.json()[1][0][3]["y"]
        return [lat, long]
    except:  # pylint: disable=bare-except
        return [None, None]


def checar_point_pertence_cidade(lat: float, long: float) -> list:
    """
    Verifica se o lat/long retornado pela API do Waze pertence ao geometry
    da cidade do Rio de Janeiro. Se pertencer retorna o lat, lon, se não retorna None.
    """

    # Acessa dados da cidade do Rio de Janeiro
    rio = read_municipality(code_muni=3304557, year=2020)

    # Cria point com a latitude e longitude
    point = Point(long, lat)

    # Cria polígono com o geometry do Rio de Janeiro
    polygon = rio.geometry

    # Use polygon.contains(point) to test if point is inside (True) or outside (False) the polygon.
    pertence = polygon.contains(point)

    # Se pertence retorna lat, lon. Do contrário retorna nan
    if pertence.iloc[0]:
        lat_lon = [lat, long]
    else:
        lat_lon = [None, None]
    return lat_lon
