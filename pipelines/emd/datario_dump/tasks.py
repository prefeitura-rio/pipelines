"""
General purpose tasks for dumping database data.
"""
import io
from pathlib import Path
from typing import Union
from datetime import datetime, timedelta

import geopandas as gpd
import pandas as pd
from prefect import task
import requests

from pipelines.utils import (
    dataframe_to_csv,
)
from pipelines.constants import constants
from pipelines.utils import log

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
):
    path = Path(path)
    request = requests.get(url)
    geodatagrame = gpd.read_file(io.BytesIO(request.content))
    dataframe = pd.DataFrame(geodatagrame)

    eventid = datetime.now().strftime("%Y%m%d-%H%M%S")
    dataframe_to_csv(dataframe, path / f"{eventid}.csv")

    return path
