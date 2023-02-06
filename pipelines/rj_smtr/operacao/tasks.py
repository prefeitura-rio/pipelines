# -*- coding: utf-8 -*-
"""
Tasks for veiculos
"""

import pandas as pd
from prefect import task
from datetime import datetime

# EMD Imports #

from pipelines.utils.utils import log  # ,get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.operacao.constants import constants

# Tasks #


@task
def pre_treatment_sppo_infracao(status: dict, timestamp: str):
    """Basic data treatment for violation data. Apply filtering.

    Args:
        status_dict (dict): dict containing the status of the request made.
        Must contain keys: data, timestamp and error

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
    - data:\n{data.head()}"""
    )

    log(f"Data raw: {df.info()}", level="info")

    # Rename columns
    columns = constants.SPPO_INFRACAO_MAPPING_KEYS.value
    data = data.rename(columns=columns)

    for col in columns.keys():
        data.col = data.col.str.strip()

    # Filter data
    filters = ["modo != 'ONIBUS'"]

    for item in filters:
        remove = data.query(item)
        data = data.drop(remove.index)
        log(f"Removed {len(remove)} rows from filter: {item}", level="info")

    # Check primary keys
    pk_columns = ["placa", "id_auto_infracao"]
    filter_new_data = (
        f"data_infracao == {datetime.strptime(timestamp).strftime('%Y-%m-%d')}"
    )

    for col in pk_columns:
        remove = data.query(f"{col} != {col}")  # null values
        data = data.drop(remove.index)

        # Check if there are important data being removed
        remove = remove.query(filter_new_data)
        if len(remove) > 0:
            log(f"Removed {len(remove)} rows with {filter_new_data}", level="warning")

    # Create nested structure
    df = data[pk_columns].copy()

    df["content"] = data[data.columns.difference(pk_columns)].apply(
        lambda x: x.to_dict(), axis=1
    )

    df["timestamp_captura"] = timestamp

    log(f"Data pre-treated: {df.info()}", level="info")

    return {"data": df, "error": error}
