# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_onibus_gps
"""

import traceback
import pandas as pd
import pendulum
from prefect import task

# EMD Imports #

from pipelines.utils.utils import log

# SMTR Imports #

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import log_critical
from pipelines.rj_smtr.br_rj_riodejaneiro_onibus_gps.utils import sppo_filters

# Tasks #


@task
def pre_treatment_br_rj_riodejaneiro_onibus_gps(status_dict: dict, version: int = 1):
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
    df["datahora"] = (
        df["datahora"]
        .astype(float)
        .apply(
            lambda ms: pd.to_datetime(
                pendulum.from_timestamp(ms / 1000.0)
                .replace(tzinfo=None)
                .set(tz="UTC")
                .isoformat()
            )
        )
    )
    log(f"After converting the timezone, datahora is: \n{df['datahora']}")
    if version == 2:
        df["datahoraenvio"] = (
            df["datahoraenvio"]
            .astype(float)
            .apply(
                lambda ms: pd.to_datetime(
                    pendulum.from_timestamp(ms / 1000.0)
                    .replace(tzinfo=None)
                    .set(tz="UTC")
                    .isoformat()
                )
            )
        )
        log(f"After converting the timezone, datahoraenvio is: \n{df['datahoraenvio']}")

    # Filter data for 0 <= time diff <= 1min
    try:
        datahora_cols = [
            "datahora",
            "datahoraenvio",
            "timestamp_captura",
        ]
        # datahora_col = "datahora"
        df_treated = df
        for col in datahora_cols:
            if col in df_treated.columns.to_list():
                try:
                    df_treated[col] = df_treated[col].apply(
                        lambda x: x.tz_convert(timezone)
                    )
                    log(f"Converted timezone on column {col}")
                except TypeError:
                    df_treated[col] = df_treated[col].apply(
                        lambda x: x.tz_localize(timezone)
                    )
                    log(f"Localized timezone on column {col}")

        # filters
        df_treated = sppo_filters(frame=df_treated, version=version)
        log(f"Shape antes da filtragem: {df.shape}")
        log(f"Shape após a filtragem: {df_treated.shape}")
        if df_treated.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
            log_critical(f"@here\nFailed to filter SPPO data: \n{error}")
        if version == 2:
            df = df_treated.drop(  # pylint: disable=C0103
                columns=["datahoraenvio", "datahoraservidor"]
            )
        else:
            df = df_treated  # pylint: disable=C0103
    except Exception:  # pylint: disable = W0703
        err = traceback.format_exc()
        log_critical(f"@here\nFailed to filter SPPO data: \n{err}")
    # log_critical(f"@here\n Got SPPO data at {timestamp} sucessfully")

    return {"df": df, "error": error}
