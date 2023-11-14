# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Any, Dict, List, Union

import geopandas as gpd
import h3
import pandas as pd
from redis_pal import RedisPal
import requests
from shapely.geometry import Point, Polygon

from pipelines.utils.utils import get_redis_client, remove_columns_accents


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


def extract_data(row: Dict[str, Any]) -> pd.Series:
    """
    Extracts username, password, and path from a given row with camera data.

    Parameters:
    - row (Dict[str, Any]): A dictionary representing a row of camera data.
      Expected keys are 'rtsp' and 'ip'.

    Returns:
    - pd.Series: A pandas Series containing extracted 'username', 'password',
      and 'path' information.
    """

    try:
        rtsp = row["rtsp"]
        # Remove protocol
        rtsp = rtsp.replace("rtsp://", "").replace("rtsp:/", "")
        # If we have an "@" in the URL, we have username and password
        if "@" in rtsp:
            # Extract username and password
            username_password = rtsp.split("@")[0].split(":")
            if len(username_password) == 2:
                username = username_password[0]
                password = username_password[1]
            else:
                print(username_password)
                raise Exception("Why???")
            # Remove username and password from rtsp
            rtsp = rtsp.split("@")[1]
        else:
            username = None
            password = None
        # Extract path
        path = "/".join(rtsp.split("/")[1:])
        # Return the data
        return pd.Series(
            {
                "username": username,
                "password": password,
                "path": path,
            }
        )
    except Exception as exc:
        print(row["rtsp"])
        raise exc


def build_rtsp(row: pd.Series) -> str:
    """
    Builds a complete RTSP URL from the given row data.

    Parameters:
    - row (pd.Series): A pandas Series containing 'username', 'password', 'path', and 'ip'.

    Returns:
    - str: The complete RTSP URL.
    """
    username = row["username"]
    password = row["password"]
    path = row["path"]
    ip = row["ip"]
    # If we have username and password, add them to the URL
    if username and password:
        return f"rtsp://{username}:{password}@{ip}/{path}"
    else:
        return f"rtsp://{ip}/{path}"


def get_rain_dataframe() -> pd.DataFrame:
    """
    Fetches and returns rainfall data from a specified API.

    Returns:
    - pd.DataFrame: A pandas DataFrame containing the rainfall data.
    """
    api_url = "https://api.dados.rio/v2/clima_pluviometro/precipitacao_15min/"
    data = requests.get(api_url).json()
    df_rain = pd.DataFrame(data)

    last_update_url = "https://api.dados.rio/v2/clima_pluviometro/ultima_atualizacao_precipitacao_15min/"  # noqa
    last_update = requests.get(last_update_url).json()
    df_rain["last_update"] = last_update
    df_rain["last_update"] = pd.to_datetime(df_rain["last_update"])

    return df_rain


def get_cameras_h3(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Enhances camera data with geographical information and joins it with rainfall data.

    Parameters:
    - df (pd.DataFrame): A DataFrame containing camera data.

    Returns:
    - gpd.GeoDataFrame: A GeoDataFrame containing the joined camera and rainfall data.
    """
    cameras = df.copy()
    geometry = [Point(xy) for xy in zip(cameras["longitude"], cameras["latitude"])]
    cameras_geo = gpd.GeoDataFrame(cameras, geometry=geometry)
    cameras_geo.crs = {"init": "epsg:4326"}

    pluviometro = get_rain_dataframe()
    pluviometro = pluviometro.rename(columns={"status": "status_chuva"})
    geometry = pluviometro["id_h3"].apply(lambda h3_id: h3_id_to_polygon(h3_id))
    pluviometro_geo = gpd.GeoDataFrame(pluviometro, geometry=geometry)
    pluviometro_geo.crs = {"init": "epsg:4326"}
    print("pluviometro_geo:", pluviometro_geo.shape)

    cameras_h3 = gpd.sjoin(cameras_geo, pluviometro_geo, how="left", op="within")
    cameras_h3 = cameras_h3.drop(columns=["index_right"])
    cameras_h3 = cameras_h3[cameras_h3["id_h3"].notnull()]

    return cameras_h3


def clean_and_padronize_cameras() -> gpd.GeoDataFrame:
    """
    Cleans and standardizes camera data from a CSV file, then merges it with geographical data.

    Returns:
    - gpd.GeoDataFrame: A GeoDataFrame containing the cleaned, standardized, and geographically
      enriched camera data.
    """
    df = pd.read_csv(
        "./data/Cameras_em_2023-11-13.csv", delimiter=";", encoding="latin1"
    )
    df.columns = remove_columns_accents(df)
    df["codigo"] = df["codigo"].str.replace("'", "")
    df = df[df["status"] == "Online"]
    df = df[df["rtsp"].str.startswith("rtsp")]

    df["ip_in_rtsp"] = df.apply(lambda row: row["ip"] in row["rtsp"], axis=1)
    df[~df["ip_in_rtsp"]]

    df["ip"] = df["ip"].replace("10.151.48.04", "10.151.48.4")
    df["ip_in_rtsp"] = df.apply(lambda row: row["ip"] in row["rtsp"], axis=1)
    df[~df["ip_in_rtsp"]]

    df[["username", "password", "path"]] = df.apply(extract_data, axis=1)
    df["rtsp"] = df.apply(build_rtsp, axis=1)
    # Filter out by subnet
    df = df[
        df["ip"].str.startswith("10.10")
        | df["ip"].str.startswith("10.151")
        | df["ip"].str.startswith("10.152")
        | df["ip"].str.startswith("10.153")
        | df["ip"].str.startswith("10.50")
        | df["ip"].str.startswith("10.52")
    ]

    cameras_h3 = get_cameras_h3(df)
    cols = [
        "codigo",
        "nome_da_camera",
        "rtsp",
        "latitude",
        "longitude",
        "geometry",
        "id_h3",
    ]
    cameras_h3 = cameras_h3[cols]

    cameras_h3 = cameras_h3.rename(
        columns={"codigo": "id_camera", "nome_da_camera": "nome"}
    )

    return cameras_h3.reset_index(drop=True)


def redis_add_to_prediction_buffer(key: str, value: bool, len_: int = 3) -> List[bool]:
    """
    Adds a value to the prediction buffer in Redis.

    Args:
        key: The Redis key.
        value: The value to be added.
        len: The length of the buffer.
    """
    prediction_buffer = redis_get_prediction_buffer(key, len_)
    prediction_buffer.append(value)
    prediction_buffer = prediction_buffer[-len_:]
    redis_client: RedisPal = get_redis_client()
    redis_client.set(key, prediction_buffer)
    return prediction_buffer


def redis_get_prediction_buffer(key: str, len_: int = 3) -> List[bool]:
    """
    Gets the prediction buffer from Redis.

    Args:
        key: The Redis key.
        len: The length of the buffer.

    Returns:
        The prediction buffer.
    """
    redis_client: RedisPal = get_redis_client()
    prediction_buffer = redis_client.get(key)
    if prediction_buffer is None:
        return [False] * len_
    elif not isinstance(prediction_buffer, list):
        return [False] * len_
    elif len(prediction_buffer) < len_:
        diff = len_ - len(prediction_buffer)
        return [False] * diff + prediction_buffer
    return prediction_buffer
