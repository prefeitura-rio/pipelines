# -*- coding: utf-8 -*-
"""
Tasks for veiculos
"""

import pandas as pd
from prefect import task

# EMD Imports #

from pipelines.utils.utils import log  # ,get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.veiculo.constants import constants

# Tasks #


@task
def pre_treatment_sppo_licenciamento(status: dict, timestamp: str):
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

    # Checar id_veiculo único
    # Checar relação única placa e id_veiculo
    # Checar relação única planta e tipo_veiculo
    # tipo_veiculo não nulo

    # Create dataframe sctructure
    key_columns = ["id_veiculo"]
    columns = key_columns + ["timestamp_captura", "content"]
    df = pd.DataFrame(columns=columns)  # pylint: disable=c0103

    data = data.rename(columns=constants.SPPO_LICENCIAMENTO_MAPPING_KEYS.value)
    df[key_columns] = data[key_columns].copy()
    df["content"] = data[data.columns.difference(key_columns)].apply(
        lambda x: x.to_dict(), axis=1
    )
    df["timestamp_captura"] = timestamp
    log(f"timestamp captura is:\n{df['timestamp_captura']}")

    return {"data": df, "error": error}
