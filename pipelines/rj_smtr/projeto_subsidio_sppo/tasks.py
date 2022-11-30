# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from prefect import task
from typing import Dict
from pathlib import Path
import zipfile
import shutil
import traceback
import pandas as pd
from functools import reduce

from pipelines.rj_smtr.constants import constants as smtr_constants

from pipelines.utils.utils import (
    get_storage_blobs,
    log,
)


@task
def get_raw(
    dataset_id: str = smtr_constants.SUBSIDIO_SPPO_PREPROD_DATASET_ID.value,
) -> Dict:
    """
    Retrieve GTFS data from GCS and saves locally without partitioning.

    Args:
        dataset_id (str, optional): Dataset ID on GCS/BigQuery

    Returns:
        dict: Conatining keys
          * `gtfs_last_modified`: Last modified date of the GTFS file
          * `error`: error catched from data retrieval
    """

    error = None

    # Download gtfs.zip + quadro.csv
    dirpath = Path(smtr_constants.SUBSIDIO_SPPO_GTFS_RAW_PATH.value)
    dirpath.mkdir(parents=True, exist_ok=True)

    blobs = get_storage_blobs(
        dataset_id=dataset_id, table_id="upload", mode="development"
    )
    log(f"Retrieved blobs: {blobs}")

    for blob in blobs:

        filename = blob.name.split("/")[-1]

        # Download single CSV file
        if filename == "quadro.csv":
            blob.download_to_filename(
                f"{dirpath.as_posix()}/{filename.split('.')[0]}.txt"
            )
            log(f"File {filename} downloaded to {dirpath.as_posix()}/{filename}")

        # Download, extract and save GTFS files from ZIP
        elif filename == "gtfs.zip":
            # Download ZIP file
            blob.download_to_filename(filename)  # filename = gtfs.zip
            log(f"Downloaded file {filename}")
            # Extract ZIP file
            with zipfile.ZipFile(filename, "r") as zip_ref:
                zip_ref.extractall(dirpath)

    gtfs_last_modified = max([blob.time_created for blob in blobs])
    log(f"GTFS time created on GCS: {gtfs_last_modified}")

    # gtfs_last_modified = "2022-11-29 20:05:40.675000+00:00"
    return {"gtfs_last_modified": gtfs_last_modified, "error": error}


@task
def save_raw_local(filepath: str, status: dict, mode: str = "raw") -> str:
    """
    Saves json response from API to .json file.

    Args:
        file_path (str): Path which to save raw file
        status (dict): Must contain keys
          * `gtfs_last_modified`: Last modified date of the GTFS file
          * `error`: error catched from data retrieval
        table_id (str): Table ID on GCS/BigQuery
        mode (str, optional): Folder to save locally, later folder which to upload to GCS.

    Returns:
        str: Path to the saved file
    """
    _file_path = filepath.format(mode=mode, filetype="txt")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)

    log(f"Set partitioned path: {_file_path}")

    if status["error"] is None:
        table_id = _file_path.split("/")[-4]
        raw_table_path = (
            f"{smtr_constants.SUBSIDIO_SPPO_GTFS_RAW_PATH.value}/{table_id}.txt"
        )
        shutil.move(raw_table_path, _file_path)
        log(f"File {table_id}.txt saved to: {_file_path}")

    return _file_path


def _treat_quadro(data: pd.DataFrame):
    """
    Reshape table from wide to long format and rename columns to match
    GTFS.

    Args:
        data (pandas.core.DataFrame): Raw dataframe.

    Returns:
        pandas.core.DataFrame: Treated dataframe.
    """

    key_columns = {
        "servico": "trip_short_name",
        "vista": "route_long_name",
        "consorcio": "agency_name",
        "horario_inicio": "start_time",
        "horario_fim": "end_time",
    }

    data = data.rename(columns=key_columns)
    key_columns = list(key_columns.values())

    # Unpivot daily distance by service days
    km_day_cols = {
        "km_dia_util": "U",
        "km_sabado": "S",
        "km_domingo": "D",
    }

    km_column = "trip_daily_distance"

    df_km_day = (
        data[key_columns + list(km_day_cols.keys())]
        .melt(key_columns, var_name="service_id", value_name=km_column)
        .dropna(subset=[km_column])
        .replace(km_day_cols)
        .drop_duplicates()
    )

    df_km_day[km_column] = (
        df_km_day[km_column]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

    # Unpivot extension by direction_id and trip_id
    km_trip_cols = {
        "trip_id": {
            "trip_id_ida": "0",
            "trip_id_volta": "1",
        },
        "shape_distance": {"extensao_ida": "0", "extensao_volta": "1"},
    }

    df_trip = []

    for col, direction_cols in km_trip_cols.items():

        df_trip.append(
            data[key_columns + list(direction_cols.keys())]
            .melt(key_columns, var_name="direction_id", value_name=col)
            .dropna(subset=[col])
            .replace(direction_cols)
            .drop_duplicates()
        )

    df_trip = reduce(
        lambda df1, df2: pd.merge(df1, df2, on=key_columns + ["direction_id"]), df_trip
    )
    df_trip["shape_distance"] = df_trip["shape_distance"].astype(float)

    # Join tables by service day
    data = df_trip.merge(df_km_day, on=key_columns, how="inner")
    return data


@task
def pre_treatment_subsidio_sppo_gtfs(
    status: Dict, filepath: str, timestamp: str
) -> Dict:
    """Basic data treatment for bus gps data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status: Dict containing keys
          * `gtfs_last_modified`: Last modified date of the GTFS file
          * `error`: error catched from data retrieval
        filepath (str): Path to downloaded raw file
        timestamp (str): Capture data timestamp.

    Returns:
        status: Dict containing keys
          * `data`:  pandas.core.DataFrame containing the treated data.
          * `error`: error catched from data retrieval
    """

    log("Check previous error")
    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    error = None

    try:
        log(f"Read CSV file: {filepath}")
        # Read CSV file
        data = pd.read_csv(filepath, sep=",")
    except Exception:  # pylint: disable = W0703
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    # Add timestamp_captura
    data["timestamp_captura"] = timestamp

    table_id = filepath.split("/")[-4]

    if table_id == "quadro":
        log(f"Treating table: {table_id}")
        data = _treat_quadro(data)

    return {"data": data, "error": error}
