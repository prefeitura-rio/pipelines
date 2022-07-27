# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_onibus_gps
"""

import traceback
import pandas as pd
from prefect import task
import pendulum
from datetime import datetime, timedelta

# EMD Imports #

from pipelines.utils.utils import log, get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import log_critical
from pipelines.rj_smtr.br_rj_riodejaneiro_onibus_gps.utils import sppo_filters

# Tasks #


@task
def create_api_url_onibus_gps(url: str, source: str, timestamp: datetime = None):
    """
    Generates the complete URL to get data from API.
    """
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
def pre_treatment_br_rj_riodejaneiro_onibus_gps(
    status_dict: dict, version: int = 1, recapture: bool = False
):
    """Basic data treatment for bus gps data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status_dict (dict): dict containing the status of the request made to the
        API. Must contain keys: data, timestamp and error
        version (int, optional): Source API version. Temporary argument for testing

    Returns:
        df: pandas.core.DataFrame containing the treated data.
    """

    if status_dict["error"] is not None:
        return {"df": pd.DataFrame(), "error": status_dict["error"]}

    error = None
    data = status_dict["data"]
    timezone = constants.TIMEZONE.value
    timestamp = status_dict["timestamp"]

    log(f"data={data.json()[:50]}")
    data = data.json()
    df = pd.DataFrame(data)  # pylint: disable=c0103
    timestamp_captura = pd.to_datetime(timestamp)
    df["timestamp_captura"] = timestamp_captura
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
        # Remove timezone and force it to be config timezone
        df[col] = (
            pd.to_datetime(df[col].astype(float), unit="ms")
            .dt.tz_localize(tz="UTC")
            .dt.tz_convert(timezone)
        )
        log(f"After converting the timezone, {col} is: \n{df[col].head()}")

    # Filter data for 0 <= time diff <= 1min
    try:
        # filters
        log(f"Shape antes da filtragem: {df.shape}")
        df = sppo_filters(  # pylint: disable=C0103
            frame=df, version=version, recapture=recapture
        )
        log(f"Shape após a filtragem: {df.shape}")
        if df.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
            log_critical(f"@here\nFailed to filter SPPO data: \n{error}")
        if version == 2:
            df = df.drop(  # pylint: disable=C0103
                columns=["datahoraenvio", "datahoraservidor"]
            )
    except Exception:  # pylint: disable = W0703
        err = traceback.format_exc()
        log_critical(f"@here\nFailed to filter SPPO data: \n{err}")
    # log_critical(f"@here\n Got SPPO data at {timestamp} sucessfully")

    return {
        "df": df.drop_duplicates(
            ["ordem", "latitude", "longitude", "datahora", "timestamp_captura"]
        ),
        "error": error,
    }
