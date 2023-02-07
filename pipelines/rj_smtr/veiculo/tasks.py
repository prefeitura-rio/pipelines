# -*- coding: utf-8 -*-
"""
Tasks for veiculos
"""

from datetime import datetime
import pandas as pd
from prefect import task

# EMD Imports #

from pipelines.utils.utils import log  # ,get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.veiculo.constants import constants
from pipelines.rj_smtr.utils import check_not_null, convert_boolean

# Tasks #


@task
def pre_treatment_sppo_licenciamento(status: dict, timestamp: datetime):
    """Basic data treatment for vehicle data. Apply filtering to raw data.

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
    log(data.info(), level="info")

    # Rename columns
    columns = constants.SPPO_LICENCIAMENTO_MAPPING_KEYS.value
    data = data.rename(columns=columns)

    data.info()

    # Strip str columns
    str_columns = data.columns[data.dtypes == "object"].to_list()
    for col in str_columns:
        data[col] = data[col].str.strip()

    # Convert to boolean
    boolean_cols = data.columns[data.columns.str.contains("indicador")].to_list()
    data = convert_boolean(
        data=data, columns=boolean_cols, dict_keys={"Sim": True, "Nao": False}
    )

    # Filter data
    # Checar id_veiculo único
    # Checar relação única placa e id_veiculo
    # Checar relação única planta e tipo_veiculo
    filters = []

    for item in filters:
        remove = data.query(item)
        data = data.drop(remove.index)
        log(f"Removed {len(remove)} rows from filter: {item}", level="info")

    # Check primary keys
    pk_columns = ["id_veiculo"]

    data = check_not_null(data, pk_columns)

    # Check relevant columns
    relevant_columns = ["tipo_veiculo"]

    data = check_not_null(data, relevant_columns)

    # Update indicador_ar_condicionado based on tipo_veiculo
    data["indicador_ar_condicionado"] = data["tipo_veiculo"].map(
        lambda x: True if "C/AR" in x.replace(" ", "") else False
    )

    # Create nested structure
    df_treated = data[pk_columns].copy()

    df_treated["content"] = data[data.columns.difference(pk_columns)].apply(
        lambda x: x.to_json(), axis=1
    )

    df_treated["timestamp_captura"] = timestamp

    log("Data pre-treated:\n", level="info")
    log(df_treated.info(), level="info")

    return {"data": df_treated, "error": error}
