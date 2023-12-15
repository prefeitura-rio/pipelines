# -*- coding: utf-8 -*-
# pylint: disable=C0103,R0914
"""
Tasks for precipitacao_alertario
"""
from datetime import timedelta
from pathlib import Path
from typing import Union, Tuple

import numpy as np
import pandas as pd
import pendulum
from prefect import task

import pandas_read_xml as pdx

# from prefect import context

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_alertario.utils import (
    parse_date_columns,
    treat_date_col,
)
from pipelines.utils.utils import (
    build_redis_key,
    compare_dates_between_tables_redis,
    log,
    to_partitions,
    save_str_on_redis,
    save_updated_rows_on_redis,
)


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def tratar_dados(
    dataset_id: str, table_id: str, mode: str = "dev"
) -> Tuple[pd.DataFrame, bool]:
    """
    Renomeia colunas e filtra dados com a hora e minuto do timestamp
    de execução mais próximo à este
    """

    url = "http://alertario.rio.rj.gov.br/upload/xml/Chuvas.xml"
    dados = pdx.read_xml(url, ["estacoes"])
    dados = pdx.fully_flatten(dados)

    drop_cols = [
        "@hora",
        "estacao|@nome",
        "estacao|@type",
        "estacao|localizacao|@bacia",
        "estacao|localizacao|@latitude",
        "estacao|localizacao|@longitude",
    ]
    rename_cols = {
        "estacao|@id": "id_estacao",
        "estacao|chuvas|@h01": "acumulado_chuva_1_h",
        "estacao|chuvas|@h04": "acumulado_chuva_4_h",
        "estacao|chuvas|@h24": "acumulado_chuva_24_h",
        "estacao|chuvas|@h96": "acumulado_chuva_96_h",
        "estacao|chuvas|@hora": "data_medicao_utc",
        "estacao|chuvas|@m15": "acumulado_chuva_15_min",
        "estacao|chuvas|@mes": "acumulado_chuva_mes",
    }

    dados = pdx.fully_flatten(dados).drop(drop_cols, axis=1).rename(rename_cols, axis=1)
    log(f"\n[DEBUG]: df.head() {dados.head()}")

    # Converte de UTC para horário São Paulo
    dados["data_medicao_utc"] = pd.to_datetime(dados["data_medicao_utc"])

    see_cols = ["data_medicao_utc", "id_estacao", "acumulado_chuva_15_min"]
    log(f"DEBUG: data utc {dados[see_cols]}")

    date_format = "%Y-%m-%d %H:%M:%S"
    dados["data_medicao"] = dados["data_medicao_utc"].dt.strftime(date_format)

    see_cols = ["data_medicao", "id_estacao", "acumulado_chuva_15_min"]

    dados.data_medicao = dados.data_medicao.apply(treat_date_col)

    # Alterando valores ND, '-' e np.nan para NULL
    dados.replace(["ND", "-", np.nan], [None, None, None], inplace=True)

    # Converte variáveis que deveriam ser float para float
    float_cols = [
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
    ]
    dados[float_cols] = dados[float_cols].apply(pd.to_numeric, errors="coerce")

    # Altera valores negativos para None
    dados[float_cols] = np.where(dados[float_cols] < 0, None, dados[float_cols])

    # Elimina linhas em que o id_estacao é igual mantendo a de menor valor nas colunas float
    dados.sort_values(["id_estacao", "data_medicao"] + float_cols, inplace=True)
    dados.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    dados["id_estacao"] = dados["id_estacao"].astype(str)

    log(f"Dataframe before comparing with last data saved on redis {dados.head()}")
    log(f"Dataframe before comparing with last data saved on redis {dados.iloc[0]}")

    dados = save_updated_rows_on_redis(
        dados,
        dataset_id,
        table_id,
        unique_id="id_estacao",
        date_column="data_medicao",
        date_format=date_format,
        mode=mode,
    )

    # Ajustando dados da meia-noite que vem sem o horário
    for index, row in dados.iterrows():
        try:
            date = pd.to_datetime(row["data_medicao"], format="%Y-%m-%d %H:%M:%S")
        except ValueError:
            date = pd.to_datetime(row["data_medicao"]) + pd.DateOffset(hours=0)

        dados.at[index, "data_medicao"] = date.strftime("%Y-%m-%d %H:%M:%S")

    # dados["data_medicao"] = dados["data_medicao"].dt.strftime("%Y-%m-%d %H:%M:%S")
    log(f"Dataframe after comparing with last data saved on redis {dados.head()}")

    if dados.shape[0] > 0:
        log(f"Dataframe after comparing with last data saved on redis {dados.iloc[0]}")

    empty_data = dados.shape[0] == 0

    # Save max date on redis to compare this with last dbt run
    if not empty_data:
        max_date = str(dados["data_medicao"].max())
        redis_key = build_redis_key(dataset_id, table_id, name="last_update", mode=mode)
        log(f"Dataframe is not empty. Redis key: {redis_key} and new date: {max_date}")
        save_str_on_redis(redis_key, "date", max_date)
    else:
        # If df is empty stop flow on flows.py
        log("Dataframe is empty. Skipping update flow.")

    # Fixar ordem das colunas
    dados = dados[
        [
            "data_medicao",
            "id_estacao",
            "acumulado_chuva_15_min",
            "acumulado_chuva_1_h",
            "acumulado_chuva_4_h",
            "acumulado_chuva_24_h",
            "acumulado_chuva_96_h",
        ]
    ]

    return dados, empty_data


@task
def salvar_dados(dados: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    prepath = Path("/tmp/precipitacao_alertario/")
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data_medicao"
    dataframe, partitions = parse_date_columns(dados, partition_column)
    current_time = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")

    # Cria partições a partir da data
    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=current_time,
    )
    log(f"Files saved on {prepath}")
    return prepath


@task
def save_last_dbt_update(
    dataset_id: str,
    table_id: str,
    mode: str = "dev",
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Save on dbt last timestamp where it was updated
    """
    now = pendulum.now("America/Sao_Paulo").to_datetime_string()
    redis_key = build_redis_key(dataset_id, table_id, name="dbt_last_update", mode=mode)
    save_str_on_redis(redis_key, "date", now)


@task(skip_on_upstream_skip=False)
def check_to_run_dbt(
    dataset_id: str,
    table_id: str,
    mode: str = "dev",
) -> bool:
    """
    It will run even if its upstream tasks skip.
    """

    key_table_1 = build_redis_key(
        dataset_id, table_id, name="dbt_last_update", mode=mode
    )
    key_table_2 = build_redis_key(dataset_id, table_id, name="last_update", mode=mode)

    format_date_table_1 = "YYYY-MM-DD HH:mm:SS"
    format_date_table_2 = "YYYY-MM-DD HH:mm:SS"

    # Returns true if date saved on table_2 (alertario) is bigger than
    # the date saved on table_1 (dbt).
    run_dbt = compare_dates_between_tables_redis(
        key_table_1, format_date_table_1, key_table_2, format_date_table_2
    )
    log(f">>>> debug data alertario > data dbt: {run_dbt}")
    return run_dbt
