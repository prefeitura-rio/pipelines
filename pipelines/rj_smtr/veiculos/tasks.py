# -*- coding: utf-8 -*-
"""
Tasks for veiculos
"""

from datetime import timedelta
import traceback
import pandas as pd
from prefect import task

# EMD Imports #

from pipelines.utils.utils import get_vault_secret, log

# SMTR Imports #

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import log_critical, map_dict_keys

# Tasks #


@task
def pre_treatment_veiculos(status: dict, timestamp: str):
    """Basic data treatment for vehicle data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status_dict (dict): dict containing the status of the request made to the
        API. Must contain keys: data, timestamp and error

    Returns:
        df: pandas.core.DataFrame containing the treated data.
    """

    # Check previous error
    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    error = None
    data = status["data"]

    log(
        f"""
    Received inputs:
    - timestamp:\n{timestamp}
    - data:\n{data[:10]}"""
    )
    # Create dataframe sctructure
    key_columns = ["placa", "data_ultima_vistoria"]
    columns = key_columns + ["timestamp_captura", "content"]
    df = pd.DataFrame(columns=columns)  # pylint: disable=c0103

    data = data.rename(columns=constants.VEHICLE_MAPPING_KEYS.value)
    df[key_columns] = data[key_columns].copy()
    df["content"] = data[data.columns.difference(key_columns)].apply(
        lambda x: x.to_dict(), axis=1
    )
    df["timestamp_captura"] = timestamp
    log(f"timestamp captura is:\n{df['timestamp_captura']}")

    return {"data": df, "error": error}
