# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_stpl_gps
"""
# pylint: disable=C0103,W0703
###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################
from datetime import timedelta
import traceback

import pandas as pd
import pendulum
from prefect import task


from pipelines.rj_smtr.constants import constants
from pipelines.utils.utils import log
from pipelines.rj_smtr.utils import log_critical


@task
def pre_treatment_br_rj_riodejaneiro_stpl_gps(status_dict):
    """Parse data from status_dict['data'] to DataFrame as partially nested table.

    Args:
        status_dict (dict): Contains data, run time timestamp and any previous error
        key_column (str): Unique key field from each dict within data

    Returns:
        dict: "df" contains the transformed DataFrame from data, "error" contains any caught error
        during execution.
    """
    key_column = "codigo"
    columns = [key_column, "dataHora", "timestamp_captura", "content"]
    data = status_dict["data"]
    timestamp = status_dict["timestamp"]

    if status_dict["error"] is not None:
        return {"df": pd.DataFrame(), "error": status_dict["error"]}

    error = None
    # get tz info from constants
    timezone = constants.TIMEZONE.value

    data = data.json()["veiculos"]

    # initialize df for nested columns
    df = pd.DataFrame(columns=columns)
    timestamp_captura = pd.to_datetime(timestamp)
    # separate each nested piece in data into a row
    df["content"] = list(data)
    # retrive key field from each nested piece in data
    df[key_column] = [piece[key_column] for piece in data]
    df["dataHora"] = [piece["dataHora"] for piece in data]
    df["timestamp_captura"] = timestamp_captura
    df["dataHora"] = df["dataHora"].apply(
        lambda ms: pd.to_datetime(
            pendulum.from_timestamp(ms / 1000.0, timezone).isoformat()
        )
    )

    # Filter data for 0 <= time diff <= 1min
    try:
        datahora_col = "dataHora"
        df_treated = df
        try:
            df_treated[datahora_col] = df_treated[datahora_col].apply(
                lambda x: x.tz_convert(timezone)
            )
        except TypeError:
            df_treated[datahora_col] = df_treated[datahora_col].apply(
                lambda x: x.tz_localize(timezone)
            )
        try:
            df_treated["timestamp_captura"] = df_treated["timestamp_captura"].apply(
                lambda x: x.tz_convert(timezone)
            )
        except TypeError:
            df_treated["timestamp_captura"] = df_treated["timestamp_captura"].apply(
                lambda x: x.tz_localize(timezone)
            )
        mask = (df_treated["timestamp_captura"] - df_treated[datahora_col]).apply(
            lambda x: timedelta(seconds=0) <= x <= timedelta(minutes=1)
        )
        df_treated = df_treated[mask]
        log(f"Shape antes da filtragem: {df.shape}")
        log(f"Shape após a filtragem: {df_treated.shape}")
        if df_treated.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
        df = df_treated
    except Exception:
        error = traceback.format_exc()
        log_critical(f"Failed to filter STPL data: \n{error}")
    return {"df": df, "error": error}
