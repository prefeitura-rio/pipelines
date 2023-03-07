# -*- coding: utf-8 -*-
"""
Tasks for veiculos
"""

from datetime import datetime
import pandas as pd
from prefect import task
import io

# EMD Imports #

from pipelines.utils.utils import log  # ,get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.veiculo.constants import constants
from pipelines.rj_smtr.utils import check_not_null, convert_boolean, check_relation

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

    # Check json
    buffer = io.StringIO()
    status["data"].info(buf=buffer)
    info_out = buffer.getvalue()

    log(f"Data raw:\n{info_out}", level="info")

    data = status["data"]

    log(
        f"""
    Received inputs:
    - timestamp:\n{timestamp}
    - data:\n{data.head()}"""
    )

    buffer = io.StringIO()
    data.info(buf=buffer)
    info_out = buffer.getvalue()

    log(f"Data raw:\n{info_out}", level="info")

    # Rename columns
    columns = constants.SPPO_LICENCIAMENTO_MAPPING_KEYS.value
    data = data.rename(columns=columns)

    # Strip str columns
    str_columns = data.columns[data.dtypes == "object"].to_list()
    for col in str_columns:
        data[col] = data[col].str.strip()

    # Convert to boolean
    boolean_cols = data.columns[data.columns.str.contains("indicador")].to_list()
    data = convert_boolean(
        data=data, columns=boolean_cols, dict_keys={"Sim": True, "Nao": False}
    )

    # Check data
    # check_columns = [["id_veiculo", "placa"], ["tipo_veiculo", "id_planta"]]

    # check_relation(data, check_columns)

    # Filter data (TBD)
    filters = ["tipo_veiculo.str.contains('ROD')"]

    for item in filters:
        remove = data.query(item)
        data = data.drop(remove.index)
        log(f"Removed {len(remove)} rows from filter: {item}", level="info")

    # Check primary keys
    pk_columns = ["id_veiculo"]

    # data = check_not_null(data, pk_columns)

    # Check relevant columns
    # relevant_columns = ["tipo_veiculo"]

    # data = check_not_null(data, relevant_columns)

    # Update indicador_ar_condicionado based on tipo_veiculo
    data["indicador_ar_condicionado"] = data["tipo_veiculo"].map(
        lambda x: True if "C/AR" in x.replace(" ", "") else False
    )

    # Add status
    data["status"] = "Licenciado"

    # Create nested structure
    df_treated = data[pk_columns].copy()

    df_treated["content"] = data[data.columns.difference(pk_columns)].apply(
        lambda x: x.to_json(), axis=1
    )

    df_treated["timestamp_captura"] = timestamp

    buffer = io.StringIO()
    df_treated.info(buf=buffer)
    info_out = buffer.getvalue()

    log(f"Data pre-treated:\n{info_out}", level="info")

    return {"data": df_treated, "error": error}


@task
def pre_treatment_sppo_infracao(status: dict, timestamp: datetime):
    """Basic data treatment for violation data. Apply filtering to raw data.

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

    buffer = io.StringIO()
    status["data"].info(buf=buffer)
    info_out = buffer.getvalue()

    log(f"Data raw:\n{info_out}", level="info")

    # Check json
    data = status["data"]

    log(
        f"""
    Received inputs:
    - timestamp:\n{timestamp}
    - data:\n{data.head()}"""
    )

    buffer = io.StringIO()
    data.info(buf=buffer)
    info_out = buffer.getvalue()

    log(f"Data raw:\n{info_out}", level="info")

    # Rename columns
    columns = constants.SPPO_INFRACAO_MAPPING_KEYS.value
    data = data.rename(columns=columns)

    # Strip str columns
    for col in columns.values():
        data[col] = data[col].str.strip()

    # Filter data
    filters = ["modo != 'ONIBUS'"]

    for item in filters:
        remove = data.query(item)
        data = data.drop(remove.index)
        log(f"Removed {len(remove)} rows from filter: {item}", level="info")

    # Update valor type to float
    data["valor"] = data["valor"].str.replace(",", ".").astype(float)

    # Check primary keys
    pk_columns = ["placa", "id_auto_infracao"]
    filter_new_data = f"data_infracao == '{timestamp.strftime('%Y-%m-%d')}'"

    data = check_not_null(data, pk_columns, subset_query=filter_new_data)

    # Create nested structure
    df_treated = data[pk_columns].copy()

    df_treated["content"] = data[data.columns.difference(pk_columns)].apply(
        lambda x: x.to_json(), axis=1
    )

    df_treated["timestamp_captura"] = timestamp

    buffer = io.StringIO()
    df_treated.info(buf=buffer)
    info_out = buffer.getvalue()

    log(f"Data pre-treated:\n{info_out}", level="info")

    return {"data": df_treated, "error": error}
