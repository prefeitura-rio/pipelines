# flake8: noqa: E501
import datetime
import pathlib

import numpy as np
import pandas as pd
import pyproj

test_stations = [5, 6, 7, 13, 18, 25, 28]
train_stations = [1, 2, 3, 4, 8, 9, 10, 11, 12, 14, 15, 16, 17, 19, 20, 21, 22, 23, 24, 26, 27, 29, 30, 31, 32, 33]

NRAYS = 360

GEODESIC = pyproj.Geod(ellps="WGS84")


def get_files_in_range(first_time_str: str, last_time_str: str, folder_path: pathlib.Path) -> list:
    first_time = datetime.datetime.strptime(first_time_str, "%H%M%S")
    last_time = datetime.datetime.strptime(last_time_str, "%H%M%S")
    difference = int((last_time - first_time).total_seconds() / (5 * 60))

    patterns = [(first_time + datetime.timedelta(minutes=5 * d)).strftime("*%H%M%S*") for d in range(difference + 1)]

    filepaths = [f for f in folder_path.iterdir() if any(f.match(p) for p in patterns)]
    filepaths.sort()
    return filepaths


def get_repeated_values(df: pd.DataFrame, columns: list, strictly_repeating: bool = True) -> list:
    df_copy = df.copy()
    column_values = [list(df_copy[col]) for col in columns]
    df_copy["zipped"] = list(zip(*column_values))
    duplicates = [g for _, g in df_copy.groupby("zipped") if len(g) > strictly_repeating]
    dicts = []

    for duplicate_df in duplicates:
        rows = duplicate_df.index.tolist()
        value = duplicate_df["zipped"][rows[0]]
        dicts.append({"value": value, "rows": rows, "columns": columns})

    return dicts


def distance(latlon1: tuple, latlon2: tuple):
    return GEODESIC.inv(latlon1[1], latlon1[0], latlon2[1], latlon2[0])[2]


def get_latlon(ref_latlon, dist, azimuth):
    lon, lat, _ = GEODESIC.fwd(ref_latlon[1], ref_latlon[0], azimuth, dist)[:2]
    return (lat, lon)


def get_close_azimuthal_grid_points(
    ref_latlon: tuple,
    point_latlon: tuple,
    epsilon: float,
    ranges: list = range(125, 250000, 250),
    azimuths: list = range(NRAYS),
):
    fwd_azimuth, back_azimuth, distance = GEODESIC.inv(ref_latlon[1], ref_latlon[0], point_latlon[1], point_latlon[0])
    if distance > epsilon:
        delta_theta = np.arcsin(epsilon / distance) * 180 / np.pi
        fwd_azimuth = (fwd_azimuth + 360) % 360
        first_range = int((distance - epsilon - ranges[0]) / (ranges[1] - ranges[0]))
        last_range = int((distance + epsilon - ranges[0]) / (ranges[1] - ranges[0])) + 1

        first_angle = int((fwd_azimuth - delta_theta - azimuths[0]) / (azimuths[1] - azimuths[0])) % 360
        last_angle = (int((fwd_azimuth + delta_theta - azimuths[0]) / (azimuths[1] - azimuths[0])) + 1) % 360
    else:
        first_range = 0
        last_range = int((distance + epsilon - ranges[0]) / (ranges[1] - ranges[0])) + 1
        first_angle = 0
        last_angle = len(azimuths) - 1
    close_points = []
    azimuth_enumeration = []
    if first_angle <= last_angle:
        azimuth_enumeration = list(enumerate(azimuths[first_angle : last_angle + 1]))
    else:
        azimuth_enumeration = list(enumerate(list(azimuths[first_angle:]) + list(azimuths[: last_angle + 1])))
    range_enumeration = list(enumerate(list(ranges[first_range : last_range + 1])))
    for i, azimuth in azimuth_enumeration:
        for j, range in range_enumeration:
            endlon, endlat = GEODESIC.fwd(ref_latlon[1], ref_latlon[0], azimuth, range)[:2]
            distance = GEODESIC.inv(point_latlon[1], point_latlon[0], endlon, endlat)[2]
            if distance < epsilon:
                close_points.append(((i + first_angle) % NRAYS, j + first_range))
    return close_points


def get_containing_square(
    ref_latlon: tuple,
    point_latlon: tuple,
    range_start: float = 125,
    range_step: float = 250,
    azimuth_start: float = 0,
    azimuth_step: float = 1,
):
    assert azimuth_start == 0 and azimuth_step == 1, "Function only implemented for az start 0 and az step 1."
    fwd_azimuth, back_azimuth, distance = GEODESIC.inv(ref_latlon[1], ref_latlon[0], point_latlon[1], point_latlon[0])
    fwd_azimuth = (360 + fwd_azimuth) % 360
    azimuth_index = int((fwd_azimuth - azimuth_start) / azimuth_step) % 360
    range_index = int((distance - range_start) / range_step)

    return [(i, j) for i in [azimuth_index, azimuth_index + 1] for j in [range_index, range_index + 1]]


def get_k_nearest_grid_neighbors(
    ref_latlon: tuple, point_latlon: tuple, k: int, ranges: list = range(125, 250000, 250), azimuths: list = range(360)
):
    delta = ranges[1] - ranges[0]
    distance = 2 * delta
    grid_points = get_close_azimuthal_grid_points(ref_latlon, point_latlon, distance, ranges, azimuths)
    while len(grid_points) < k:
        distance = distance + delta
        grid_points = get_close_azimuthal_grid_points(ref_latlon, point_latlon, distance, ranges, azimuths)
    grid_points.sort(
        key=lambda x: GEODESIC.inv(
            point_latlon[1],
            point_latlon[0],
            *(GEODESIC.fwd(ref_latlon[1], ref_latlon[0], azimuths[x[0]], ranges[x[1]])[0:2]),
        )[2],
    )

    return grid_points[:k]


def get_k_nearest_directed_grid_neighbors(
    ref_latlon: tuple, point_latlon: tuple, k: int, ranges: list = range(125, 250000, 250), azimuths: list = range(360)
):
    assert list(azimuths) == list(range(360)), "Function only implemented for azimuths from 0 to 359, step 1."
    delta = ranges[1] - ranges[0]
    distance = delta
    grid_points = []
    while len(grid_points) < k:
        distance = distance + delta
        grid_points = get_close_azimuthal_grid_points(ref_latlon, point_latlon, distance, ranges, azimuths)
        distances = []
        angles = []
        for point in grid_points:
            angle, distance = GEODESIC.inv(
                point_latlon[1],
                point_latlon[0],
                *(GEODESIC.fwd(ref_latlon[1], ref_latlon[0], azimuths[point[0]], ranges[point[1]])[0:2]),
            )[::2]
            angle_bin = int(((angle + 180 / k) % 360) * k / 360)
            distances.append(distance)
            angles.append(angle_bin)

        grid_points = list(zip(grid_points, distances, angles))

        missing_angle_bins = list(range(k))
        grid_points.sort(
            key=lambda x: x[1],
        )
        new_list = []
        for point in grid_points:
            try:
                missing_angle_bins.remove(point[2])
                new_list.append(point)
            except ValueError:
                continue
        grid_points = new_list

    grid_points.sort(key=lambda x: x[2])
    grid_points = [point[0] for point in grid_points]
    return grid_points
