# -*- coding: utf-8 -*-
"""
General purpose tasks for dumping database data.
"""
# pylint disable=unused-argument, W0613

import io
from pathlib import Path
from typing import Union
from datetime import datetime, timedelta

import geopandas as gpd
import pandas as pd
from prefect import task
import requests

from pipelines.utils.utils import (
    dataframe_to_csv,
    log,
    remove_columns_accents,
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
    wait=None,  # pylint disable=unused-argument
):
    """ "
    Save a CSV from data.rio API
    """
    path = Path(path)
    request = requests.get(url)
    geodatagrame = gpd.read_file(io.BytesIO(request.content))
    dataframe = pd.DataFrame(geodatagrame)

    eventid = datetime.now().strftime("%Y%m%d-%H%M%S")

    log(f"Original columns: {dataframe.columns.tolist()}")
    dataframe.columns = remove_columns_accents(dataframe)
    log(f"New columns: {dataframe.columns.tolist()}")

    dataframe_to_csv(dataframe, path / f"{eventid}.csv")

    return path
