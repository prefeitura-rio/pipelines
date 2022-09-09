# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_onibus_gps
"""

import traceback
from datetime import datetime, timedelta
from typing import Dict
import pandas as pd
from prefect import task
import pendulum

# EMD Imports #

from pipelines.utils.utils import log, get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

# Tasks #


@task
def create_api_url_onibus_gps(version: str, timestamp: datetime = None) -> str:
    """
    Generates the complete URL to get data from API.
    """

    if version == 2:
        url = constants.GPS_SPPO_API_BASE_URL_V2.value
        source = constants.GPS_SPPO_API_SECRET_PATH_V2.value
    if version == 1:
        url = constants.GPS_SPPO_API_BASE_URL.value
        source = constants.GPS_SPPO_API_SECRET_PATH.value

    if not timestamp:
        timestamp = pendulum.now(constants.TIMEZONE.value).replace(
            second=0, microsecond=0
        )

    headers = get_vault_secret(source)["data"]
    key = list(headers)[0]
    url = f"{url}{key}={{secret}}"

    if source == "sppo_api_v2":
        date_range = {
            "start": (timestamp - timedelta(minutes=6)).strftime("%Y-%m-%d+%H:%M:%S"),
            "end": (timestamp - timedelta(minutes=5)).strftime("%Y-%m-%d+%H:%M:%S"),
        }
        url += f"&dataInicial={date_range['start']}&dataFinal={date_range['end']}"

    log(f"Request data from URL: {url}")
    return url.format(secret=headers[key])


@task
# pylint: disable=too-many-locals
def pre_treatment_br_rj_riodejaneiro_onibus_gps(
    status: dict, timestamp: datetime, version: int = 1, recapture: bool = False
) -> Dict:
    """Basic data treatment for bus gps data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status_dict (dict): dict containing the status of the request made to the
        API. Must contain keys: data and error
        version (int, optional): Source API version. Temporary argument
        for testing
        timestamp (str): Capture data timestamp.

    Returns:
        df: pandas.core.DataFrame containing the treated data.
    """
    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    if status["data"] == []:
        log("Data is empty, skipping treatment...")
        return {"data": pd.DataFrame(), "error": status["error"]}

    error = None
    timezone = constants.TIMEZONE.value

    log(f"Data received to treat: \n{status['data'][:5]}")
    df = pd.DataFrame(status["data"])  # pylint: disable=c0103
    df["timestamp_captura"] = timestamp
    log(f"Before converting, datahora is: \n{df['datahora']}")

    # Remove timezone and force it to be config timezone
    if version == 1:
        timestamp_cols = ["datahora"]
    elif version == 2:
        timestamp_cols = ["datahora", "datahoraenvio"]
    if recapture:
        timestamp_cols.append("datahoraservidor")
    for col in timestamp_cols:
        print(f"Before converting, {col} is: \n{df[col].head()}")  # log
        df[col] = (
            pd.to_datetime(df[col].astype(float), unit="ms")
            .dt.tz_localize(tz="UTC")
            .dt.tz_convert(timezone)
        )
        log(f"After converting the timezone, {col} is: \n{df[col].head()}")

    # Filter data
    try:
        log(f"Shape before filtering: {df.shape}")
        if version == 1:
            filter_col = "timestamp_captura"
            time_delay = constants.GPS_SPPO_CAPTURE_DELAY_V1.value
        elif version == 2:
            filter_col = "datahoraenvio"
            time_delay = constants.GPS_SPPO_CAPTURE_DELAY_V2.value
        if recapture:
            server_mask = (df["datahoraenvio"] - df["datahoraservidor"]) <= timedelta(
                minutes=constants.GPS_SPPO_RECAPTURE_DELAY_V2.value
            )
            df = df[server_mask]  # pylint: disable=c0103

        mask = (df[filter_col] - df["datahora"]).apply(
            lambda x: timedelta(seconds=0) <= x <= timedelta(minutes=time_delay)
        )

        # Select and drop duplicated data
        cols = [
            "ordem",
            "latitude",
            "longitude",
            "datahora",
            "velocidade",
            "linha",
            "timestamp_captura",
        ]
        df = df[mask][cols]  # pylint: disable=c0103
        df = df.drop_duplicates(  # pylint: disable=c0103
            ["ordem", "latitude", "longitude", "datahora", "timestamp_captura"]
        )

        log(f"Shape after filtering: {df.shape}")
        if df.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")
    except Exception:  # pylint: disable = W0703
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": df, "error": error}
