# -*- coding: utf-8 -*-
"""
Tasks for operacao
"""

import pandas as pd
from prefect import task
from datetime import datetime

# EMD Imports #

from pipelines.utils.utils import log  # ,get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.operacao.constants import constants
from pipelines.rj_smtr.utils import check_not_null

# Tasks #


@task
def pre_treatment_sppo_infracao(status: dict, timestamp: datetime):
    """Basic data treatment for violation data. Apply filtering.

    Args:
        status_dict (dict): dict containing the status of the request made.
        Must contain keys: data, timestamp and error
        timestamp (datetime): timestamp of the data capture

    Returns:
        dict: dict containing the data treated and the current error status.
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

    log("Data raw:\n", level="info")
    data.info()

    # Rename columns
    columns = constants.SPPO_INFRACAO_MAPPING_KEYS.value
    data = data.rename(columns=columns)

    for col in columns.values():
        data[col] = data[col].str.strip()

    # Filter data
    filters = ["modo != 'ONIBUS'"]

    for item in filters:
        remove = data.query(item)
        data = data.drop(remove.index)
        log(f"Removed {len(remove)} rows from filter: {item}", level="info")

    # Check primary keys
    pk_columns = ["placa", "id_auto_infracao"]
    filter_new_data = f"data_infracao == '{timestamp.strftime('%Y-%m-%d')}'"

    data = check_not_null(data, pk_columns, subset_query=filter_new_data)

    # Create nested structure
    df = data[pk_columns].copy()

    df["content"] = data[data.columns.difference(pk_columns)].apply(
        lambda x: x.to_dict(), axis=1
    )

    df["timestamp_captura"] = timestamp

    log("Data pre-treated:\n", level="info")
    df.info()

    return {"data": df, "error": error}
