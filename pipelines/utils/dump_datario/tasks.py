# -*- coding: utf-8 -*-
"""
General purpose tasks for dumping database data.
"""
# pylint: disable=unused-argument, W0613, R0913, W0108,

from pathlib import Path
from typing import Union
from datetime import datetime, timedelta

from geojsplit import geojsplit
import geopandas as gpd
from prefect import task
import requests

from pipelines.utils.utils import log, log_mod, remove_columns_accents

from pipelines.utils.dump_datario.utils import (
    load_wkt,
    remove_third_dimension,
)
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
    wait=None,
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

    return file_path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def transform_geodataframe(
    file_path: Union[str, Path],
    batch_size: int = 50000,
    geometry_column: str = "geometry",
    convert_to_crs_4326: bool = False,
    geometry_3d_to_2d: bool = False,
    wait=None,
):  # sourcery skip: convert-to-enumerate
    """ "
    Transform a CSV from data.rio API
    """
    eventid = datetime.now().strftime("%Y%m%d-%H%M%S")

    # move to path file since file_path is path / "geo_data" / "data.geojson"
    save_path = file_path.parent.parent / "csv_data" / f"{eventid}.csv"
    save_path.parent.mkdir(parents=True, exist_ok=True)

    geojson = geojsplit.GeoJSONBatchStreamer(file_path)

    # only print every print_mod batches
    mod = 100
    for index, feature_collection in enumerate(geojson.stream(batch=batch_size)):
        count = index + 1
        geodataframe = gpd.GeoDataFrame.from_features(feature_collection["features"])
        log_mod(
            msg=f"{count} x {batch_size} rows: geodataframe loaded",
            index=count,
            mod=mod,
        )

        # move geometry column to the end
        cols = geodataframe.columns.tolist()
        cols.remove(geometry_column)
        cols.append(geometry_column)
        geodataframe = geodataframe[cols]

        # remove accents from columns
        geodataframe.columns = remove_columns_accents(geodataframe)
        geodataframe["geometry_wkt"] = geodataframe[geometry_column].copy()

        # convert geometry to crs 4326
        if convert_to_crs_4326:
            try:
                geodataframe.crs = "epsg:4326"
                geodataframe[geometry_column] = geodataframe[geometry_column].to_crs(
                    "epsg:4326"
                )
            except Exception as err:
                log(f"{count}: error converting to crs 4326: {err}")
                raise err

            log_mod(
                msg=f"{count}: geometry converted to crs 4326",
                index=count,
                mod=mod,
            )

        # convert geometry 3d to 2d
        if geometry_3d_to_2d:
            try:
                geodataframe[geometry_column] = (
                    geodataframe[geometry_column].astype(str).apply(load_wkt)
                )

                geodataframe[geometry_column] = geodataframe[geometry_column].apply(
                    remove_third_dimension
                )
            except Exception as err:
                log(f"{count}: error converting 3d to 2d: {err}")
                raise err

            log_mod(
                msg=f"{count}: geometry converted 3D to 2D",
                index=count,
                mod=mod,
            )

        log_mod(f"{count}: new columns: {geodataframe.columns.tolist()}")

        # save geodataframe to csv
        geodataframe.to_csv(
            save_path,
            index=False,
            encoding="utf-8",
            mode="a",
            header=not save_path.exists(),
        )

        # clear memory
        del geodataframe

        log_mod(
            msg=f"{count} x {batch_size} rows: Data saved",
            index=count,
            mod=mod,
        )

    return save_path
