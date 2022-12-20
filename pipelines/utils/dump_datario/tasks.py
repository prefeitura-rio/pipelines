# -*- coding: utf-8 -*-
"""
General purpose tasks for dumping database data.
"""
# pylint: disable=unused-argument, W0613, R0913, W0108,

from pathlib import Path
from typing import Union
from datetime import datetime, timedelta

import pandas as pd
import geopandas as gpd
from prefect import task
import requests

from pipelines.utils.utils import (
    log,
    remove_columns_accents,
)

from pipelines.utils.dump_datario.utils import remove_third_dimension, load_wkt
from pipelines.constants import constants

###############
#
# File
#
###############


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
    nout=2,
)
def get_datario_geodataframe(
    url: str,
    path: Union[str, Path],
    wait=None,  # pylint: disable=unused-argument
):
    """ "
    Save a CSV from data.rio API
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / "geo_data" / "data.geojson"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    req = requests.get(url, stream=True)
    with open(file_path, "wb") as file:
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)
                file.flush()

    log("Data saved")

    return file_path, path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def transform_geodataframe(
    file_path: Union[str, Path],
    path: Union[str, Path],
    chunksize: int = 50000,
    geometry_column: str = "geometry",
    convert_to_crs_4326: bool = False,
    geometry_3d_to_2d: bool = False,
    wait=None,  # pylint: disable=unused-argument
):
    """ "
    Transform a CSV from data.rio API
    """
    eventid = datetime.now().strftime("%Y%m%d-%H%M%S")
    save_path = path / "csv_data" / f"{eventid}.csv"
    save_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.read_csv(file_path, chunksize=chunksize) as reader:
        for index, dataframe in enumerate(reader):
            geodataframe = gpd.GeoDataFrame(dataframe)
            log(f"{index}: Geodatagrame loaded")
            log(f"{index}: Original columns: {geodataframe.columns.tolist()}")
            geodataframe.columns = remove_columns_accents(geodataframe)
            log(f"{index}: New columns: {geodataframe.columns.tolist()}")

            geodataframe["geometry_wkt"] = geodataframe[geometry_column].copy()

            if convert_to_crs_4326:

                try:
                    geodataframe.crs = "epsg:4326"
                    geodataframe[geometry_column] = geodataframe[
                        geometry_column
                    ].to_crs("epsg:4326")
                except Exception as err:
                    log(f"{index}:Error converting to crs 4326: {err}")
                    raise err

                log(f"{index}: geometry converted to crs 4326")

            if geometry_3d_to_2d:
                try:
                    geodataframe[geometry_column] = (
                        geodataframe[geometry_column].astype(str).apply(load_wkt)
                    )

                    geodataframe[geometry_column] = geodataframe[geometry_column].apply(
                        lambda geom: remove_third_dimension(geom)
                    )
                except Exception as err:
                    log(f"{index}: Error converting 3d to 2d: {err}")
                    raise err

                log("geometry converted 3D to 2D")

            log(f"{index}:  New columns: {geodataframe.columns.tolist()}")

            geodataframe.to_csv(
                save_path,
                index=False,
                encoding="utf-8",
                mode="a",
                header=not save_path.exists(),
            )
            log(f"{index}: Data saved")

    return save_path
