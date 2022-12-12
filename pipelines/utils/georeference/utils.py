# -*- coding: utf-8 -*-
"""
Helper functions for georeferencing
"""

from geobr import read_municipality
from shapely.geometry import Point


def check_if_belongs_to_rio(lat: float, long: float) -> list:
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
