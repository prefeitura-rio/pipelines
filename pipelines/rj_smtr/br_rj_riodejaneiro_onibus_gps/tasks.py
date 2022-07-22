# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_onibus_gps
"""

import traceback
import pandas as pd
from prefect import task
from datetime import timedelta

# EMD Imports #

from pipelines.utils.utils import log

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

# Tasks #


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

    # Check for previous errors
    if status_dict["error"] is not None:
        return {"df": pd.DataFrame(), "error": status_dict["error"]}
    else:
        error = None

    data = status_dict["data"]
    timestamp = status_dict["timestamp"]

    log(f"data={data[:50]}")
    df = pd.DataFrame(data)  # pylint: disable=c0103
    df["timestamp_captura"] = pd.to_datetime(timestamp)

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
            .dt.tz_convert(constants.TIMEZONE.value)
        )
        log(f"After converting the timezone, {col} is: \n{df[col].head()}")

    # Filter data
    try:
        log(f"Shape before filtering: {df.shape}")
        # Filter capture time delays
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
            df = df[server_mask]

        mask = (df[filter_col] - df["datahora"]).apply(
            lambda x: timedelta(seconds=0) <= x <= timedelta(minutes=time_delay)
        )
        df = df[mask]

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
        df = df[cols].drop_duplicates(["ordem", "datahora"])

        log(f"Shape after filtering: {df.shape}")

        if df.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
            log(error)

    except Exception:  # pylint: disable = W0703
        error = traceback.format_exc()
        log(error)

    return {"df": df, "error": error, "timestamp": timestamp}
