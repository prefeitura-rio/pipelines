# -*- coding: utf-8 -*-
"""
General purpose tasks for dumping database data.
"""
# pylint disable=unused-argument, W0613

from pathlib import Path
from typing import Union
from datetime import datetime, timedelta

import geopandas as gpd
from prefect import task
import requests
from shapely import wkt

from pipelines.utils.utils import (
    log,
    remove_columns_accents,
)

from pipelines.utils.dump_datario.utils import remove_third_dimension
from pipelines.constants import constants

###############
#
# File
#
###############


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_datario_geodataframe(
    url: str,
    path: Union[str, Path],
    geometry_column: str = "geometry",
    convert_to_crs_4326: bool = False,
    geometry_3d_to_2d: bool = False,
    wait=None,  # pylint: disable=unused-argument
):
    """ "
    Save a CSV from data.rio API
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)

    filepath = path / "geo_data" / "data.geojson"
    filepath.parent.mkdir(parents=True, exist_ok=True)

    req = requests.get(url, stream=True)
    with open(filepath, "wb") as file:
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)
                file.flush()

    log("Data saved")

    geodataframe = gpd.read_file(filepath)
    log("Geodatagrame loaded")

    eventid = datetime.now().strftime("%Y%m%d-%H%M%S")

    log(f"Original columns: {geodataframe.columns.tolist()}")
    geodataframe.columns = remove_columns_accents(geodataframe)
    log(f"New columns: {geodataframe.columns.tolist()}")

    ## Flat column geometry  to crs 4326

    if convert_to_crs_4326:
        geodataframe["geometry_wkt"] = geodataframe[geometry_column].copy()

        try:
            geodataframe.crs = "epsg:4326"
            geodataframe[geometry_column] = geodataframe[geometry_column].to_crs(
                "epsg:4326"
            )
        except Exception as e:
            log(f"Error converting to crs 4326: {e}")
            raise e

    if geometry_3d_to_2d:
        try:
            geodataframe[geometry_column] = (
                geodataframe[geometry_column].astype(str).apply(wkt.loads)
            )

            geodataframe[geometry_column] = geodataframe[geometry_column].apply(
                lambda geom: remove_third_dimension(geom)
            )
        except Exception as e:
            log(f"Error converting 3d to 2d: {e}")
            raise e

    save_path = path / "csv_data" / f"{eventid}.csv"
    save_path.parent.mkdir(parents=True, exist_ok=True)
    geodataframe.to_csv(save_path, index=False, encoding="utf-8")
    log("Data saved")

    return save_path
