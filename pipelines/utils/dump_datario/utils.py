# -*- coding: utf-8 -*-
# pylint: disable=R0914, W0622, R1705, R1704, R0911, C0103, R0911, R0912, W0703, W0612
"""
General utilities for interacting with datario-dump
"""

from datetime import timedelta, datetime
from typing import List

from shapely.geometry import (
    Polygon,
    MultiPolygon,
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    Point,
    GeometryCollection,
)
from shapely import wkt


from prefect.schedules.clocks import IntervalClock


def generate_dump_datario_schedules(  # pylint: disable=too-many-arguments,too-many-locals
    interval: timedelta,
    start_date: datetime,
    labels: List[str],
    table_parameters: dict,
    runs_interval_minutes: int = 15,
) -> List[IntervalClock]:
    """
    Generates multiple schedules for dumping datario tables.
    """
    clocks = []
    for count, (table_id, parameters) in enumerate(table_parameters.items()):
        parameter_defaults = {
            "url": parameters["url"],
            "dataset_id": parameters["dataset_id"],
            "dump_mode": parameters["dump_mode"],
            "table_id": table_id,
        }
        if "materialize_after_dump" in parameters:
            parameter_defaults["materialize_after_dump"] = parameters[
                "materialize_after_dump"
            ]
        if "materialization_mode" in parameters:
            parameter_defaults["materialization_mode"] = parameters[
                "materialization_mode"
            ]
        if "geometry_column" in parameters:
            parameter_defaults["geometry_column"] = parameters["geometry_column"]
        if "convert_to_crs_4326" in parameters:
            parameter_defaults["convert_to_crs_4326"] = parameters[
                "convert_to_crs_4326"
            ]
        if "geometry_3d_to_2d" in parameters:
            parameter_defaults["geometry_3d_to_2d"] = parameters["geometry_3d_to_2d"]

        new_interval = parameters["interval"] if "interval" in parameters else interval
        clocks.append(
            IntervalClock(
                interval=new_interval,
                start_date=start_date
                + timedelta(minutes=runs_interval_minutes * count),
                labels=labels,
                parameter_defaults=parameter_defaults,
            )
        )
    return clocks


def remove_third_dimension(geom):
    """
    Remove third dimension from geometry
    """
    if geom is None:
        return None

    if geom.is_empty:
        return geom

    if isinstance(geom, Polygon):
        exterior = geom.exterior
        new_exterior = remove_third_dimension(exterior)

        interiors = geom.interiors
        new_interiors = []
        for int in interiors:
            new_interiors.append(remove_third_dimension(int))

        return Polygon(new_exterior, new_interiors)

    elif isinstance(geom, LinearRing):
        return LinearRing([xy[0:2] for xy in list(geom.coords)])

    elif isinstance(geom, LineString):
        return LineString([xy[0:2] for xy in list(geom.coords)])

    elif isinstance(geom, Point):
        return Point([xy[0:2] for xy in list(geom.coords)])

    elif isinstance(geom, MultiPoint):
        points = list(geom.geoms)
        new_points = []
        for point in points:
            new_points.append(remove_third_dimension(point))

        return MultiPoint(new_points)

    elif isinstance(geom, MultiLineString):
        lines = list(geom.geoms)
        new_lines = []
        for line in lines:
            new_lines.append(remove_third_dimension(line))

        return MultiLineString(new_lines)

    elif isinstance(geom, MultiPolygon):
        pols = list(geom.geoms)

        new_pols = []
        for pol in pols:
            new_pols.append(remove_third_dimension(pol))

        return MultiPolygon(new_pols)

    elif isinstance(geom, GeometryCollection):
        geoms = list(geom.geoms)

        new_geoms = []
        for geom in geoms:
            new_geoms.append(remove_third_dimension(geom))

        return GeometryCollection(new_geoms)

    else:
        raise RuntimeError(
            "Currently this type of geometry is not supported: {}".format(type(geom))
        )


def load_wkt(x):
    """
    Fromt text to geometry
    """
    try:
        return wkt.loads(x)
    except Exception as err:
        return None
