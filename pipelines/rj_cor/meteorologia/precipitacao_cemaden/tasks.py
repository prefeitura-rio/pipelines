# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Tasks for precipitacao_cemaden
"""
from datetime import timedelta
from pathlib import Path
from typing import Union, Tuple

import numpy as np
import pandas as pd
import pendulum
from prefect import task

# from prefect import context

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_cemaden.utils import (
    parse_date_columns,
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

    url = "http://sjc.salvar.cemaden.gov.br/resources/graficos/interativo/getJson2.php?uf=RJ"
    dados = pd.read_json(url)

    drop_cols = [
        "uf",
        "codibge",
        "cidade",
        "tipoestacao",
        "status",
    ]
    rename_cols = {
        "idestacao": "id_estacao",
        "nomeestacao": "nome_estacao",
        "ultimovalor": "instantaneo_chuva",
        "datahoraUltimovalor": "data_medicao_utc",
        "acc1hr": "acumulado_chuva_1_h",
        "acc3hr": "acumulado_chuva_3_h",
        "acc6hr": "acumulado_chuva_6_h",
        "acc12hr": "acumulado_chuva_12_h",
        "acc24hr": "acumulado_chuva_24_h",
        "acc48hr": "acumulado_chuva_48_h",
        "acc72hr": "acumulado_chuva_72_h",
        "acc96hr": "acumulado_chuva_96_h",
    }

    dados = (
        dados[(dados["codibge"] == 3304557) & (dados["tipoestacao"] == 1)]
        .drop(drop_cols, axis=1)
        .rename(rename_cols, axis=1)
    )
    log(f"\n[DEBUG]: df.head() {dados.head()}")

    # Converte de UTC para horário São Paulo
    dados["data_medicao_utc"] = pd.to_datetime(dados["data_medicao_utc"], dayfirst=True)

    see_cols = ["data_medicao_utc", "id_estacao", "acumulado_chuva_1_h"]
    log(f"DEBUG: data utc {dados[see_cols]}")

    date_format = "%Y-%m-%d %H:%M:%S"
    dados["data_medicao"] = dados["data_medicao_utc"].dt.strftime(date_format)

    log(f"DEBUG: df dtypes {dados.dtypes}")
    see_cols = ["data_medicao", "id_estacao", "acumulado_chuva_1_h"]
    log(f"DEBUG: data {dados[see_cols]}")

    # Alterando valores '-' e np.nan para NULL
    dados.replace(["-", np.nan], [None], inplace=True)

    # Altera valores negativos para None
    float_cols = [
        "acumulado_chuva_1_h",
        "acumulado_chuva_3_h",
        "acumulado_chuva_6_h",
        "acumulado_chuva_12_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_48_h",
        "acumulado_chuva_72_h",
        "acumulado_chuva_96_h",
    ]
    dados[float_cols] = np.where(dados[float_cols] < 0, None, dados[float_cols])

    # Elimina linhas em que o id_estacao é igual mantendo a de menor valor nas colunas float
    dados.sort_values(["id_estacao", "data_medicao"] + float_cols, inplace=True)
    dados.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    log(f"uniquesss df >>>, {type(dados.id_estacao.unique()[0])}")
    dados["id_estacao"] = dados["id_estacao"].astype(str)

    dados = save_updated_rows_on_redis(
        dados,
        dataset_id,
        table_id,
        unique_id="id_estacao",
        date_column="data_medicao",
        date_format=date_format,
        mode=mode,
    )

    # If df is empty stop flow on flows.py
    empty_data = dados.shape[0] == 0
    log(f"[DEBUG]: dataframe is empty: {empty_data}")

    # Save max date on redis to compare this with last dbt run
    if not empty_data:
        max_date = str(dados["data_medicao"].max())
        redis_key = build_redis_key(dataset_id, table_id, name="last_update", mode=mode)
        log(f"[DEBUG]: dataframe is not empty key: {redis_key} {max_date}")
        save_str_on_redis(redis_key, "date", max_date)

    # Fixar ordem das colunas
    dados = dados[
        [
            "data_medicao",
            "id_estacao",
            "nome_estacao",
            "instantaneo_chuva",
            "acumulado_chuva_1_h",
            "acumulado_chuva_3_h",
            "acumulado_chuva_6_h",
            "acumulado_chuva_12_h",
            "acumulado_chuva_24_h",
            "acumulado_chuva_48_h",
            "acumulado_chuva_72_h",
            "acumulado_chuva_96_h",
        ]
    ]

    return dados, empty_data


@task
def salvar_dados(dados: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    prepath = Path("/tmp/precipitacao_cemaden/")
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
    log(f"[DEBUG] Files saved on {prepath}")
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
    log(f">>>>> debug saving actual date on dbt redis {redis_key} {now}")
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

    # Returns true if date saved on table_2 (cemaden) is bigger than
    # the date saved on table_1 (dbt).
    run_dbt = compare_dates_between_tables_redis(
        key_table_1, format_date_table_1, key_table_2, format_date_table_2
    )
    log(f">>>> debug data cemaden > data dbt: {run_dbt}")
    return run_dbt
