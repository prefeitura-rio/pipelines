# -*- coding: utf-8 -*-
"""
Tasks for veiculos
"""
import traceback
from datetime import datetime
import pandas as pd
import numpy as np
from prefect import task

# EMD Imports #

from pipelines.utils.utils import log  # ,get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import (
    data_info_str,
    filter_data,
)

# Tasks #


@task
def get_raw_from_file(filepath: str, csv_args: dict):
    data = None
    error = None
    try:
        data = pd.read_csv(filepath, **csv_args).to_dict(orient="records")
    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": data, "error": error}


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

    try:
        error = None
        data = pd.json_normalize(status["data"])

        log(
            f"""
        Received inputs:
        - timestamp:\n{timestamp}
        - data:\n{data.head()}"""
        )

        log(f"Raw data:\n{data_info_str(data)}", level="info")

        log("Renaming columns...", level="info")
        data = data.rename(columns=constants.SPPO_LICENCIAMENTO_MAPPING_KEYS.value)

        log("Adding captured timestamp column...", level="info")
        data["timestamp_captura"] = timestamp

        log("Striping string columns...", level="info")
        for col in data.columns[data.dtypes == "object"].to_list():
            data[col] = data[col].str.strip()

        log("Converting boolean values...", level="info")
        for col in data.columns[data.columns.str.contains("indicador")].to_list():
            data[col] = data[col].map({"Sim": True, "Nao": False})

        # Check data
        # check_columns = [["id_veiculo", "placa"], ["tipo_veiculo", "id_planta"]]

        # check_relation(data, check_columns)

        log("Filtering null primary keys...", level="info")
        primary_key = ["id_veiculo"]
        data.dropna(subset=primary_key, inplace=True)

        log("Update indicador_ar_condicionado based on tipo_veiculo...", level="info")
        data["indicador_ar_condicionado"] = data["tipo_veiculo"].map(
            lambda x: (
                None if not isinstance(x, str) else bool("C/AR" in x.replace(" ", ""))
            )
        )

        log("Update status...", level="info")
        data["status"] = "Licenciado"

        log(
            f"Finished cleaning! Pre-treated data:\n{data_info_str(data)}", level="info"
        )

        log("Creating nested structure...", level="info")
        pk_cols = primary_key + ["timestamp_captura"]
        data = (
            data.groupby(pk_cols)
            .apply(
                lambda x: x[data.columns.difference(pk_cols)].to_json(orient="records")
            )
            .str.strip("[]")
            .reset_index(name="content")[primary_key + ["content", "timestamp_captura"]]
        )

        log(
            f"Finished nested structure! Pre-treated data:\n{data_info_str(data)}",
            level="info",
        )

    except Exception as exp:  # pylint: disable=W0703
        error = exp

    if error is not None:
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": data, "error": error}


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

    try:
        error = None
        data = pd.json_normalize(status["data"])

        log(
            f"""
        Received inputs:
        - timestamp:\n{timestamp}
        - data:\n{data.head()}"""
        )

        log(f"Raw data:\n{data_info_str(data)}", level="info")

        log("Renaming columns...", level="info")
        data = data.rename(columns=constants.SPPO_INFRACAO_MAPPING_KEYS.value)

        log("Adding captured timestamp column...", level="info")
        data["timestamp_captura"] = timestamp

        log("Striping string columns and replacing empty strings...", level="info")
        for col in data.columns[data.dtypes == "object"].to_list():
            data[col] = data[col].str.strip().replace("", np.nan)

        log("Updating valor type to float...", level="info")
        data["valor"] = data["valor"].str.replace(",", ".").astype(float)

        filters = ["modo != 'ONIBUS'"]
        log(f"Filtering '{filters}'...", level="info")
        data = filter_data(data, filters)

        log("Filtering null primary keys...", level="info")
        primary_key = ["placa", "id_auto_infracao"]
        data.dropna(subset=primary_key, inplace=True)

        # Check primary keys
        # pk_columns = ["placa", "id_auto_infracao"]
        # check_new_data = f"data_infracao == '{timestamp.strftime('%Y-%m-%d')}'"
        # check_not_null(data, pk_columns, subset_query=check_new_data)

        log(
            f"Finished cleaning! Pre-treated data:\n{data_info_str(data)}", level="info"
        )

        log("Creating nested structure...", level="info")
        pk_cols = primary_key + ["timestamp_captura"]
        data = (
            data.groupby(pk_cols)
            .apply(
                lambda x: x[data.columns.difference(pk_cols)].to_json(orient="records")
            )
            .str.strip("[]")
            .reset_index(name="content")[primary_key + ["content", "timestamp_captura"]]
        )

        log(
            f"Finished nested structure! Pre-treated data:\n{data_info_str(data)}",
            level="info",
        )

    except Exception as exp:  # pylint: disable=W0703
        error = exp

    return {"data": data, "error": error}
