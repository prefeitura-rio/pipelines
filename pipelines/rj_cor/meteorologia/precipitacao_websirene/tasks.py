# -*- coding: utf-8 -*-
"""
Tasks for precipitacao_alertario
"""
from datetime import timedelta
from pathlib import Path
from typing import Union, Tuple

import pandas as pd
import pendulum
from prefect import task
import pandas_read_xml as pdx

from pipelines.constants import constants
from pipelines.utils.utils import (
    log,
    to_partitions,
    parse_date_columns,
    save_updated_rows_on_redis,
)


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_dados() -> pd.DataFrame:
    """
    Faz o request e salva em um dataframe
    """

    # Acessar url websirene
    url = "http://websirene.rio.rj.gov.br/xml/chuvas.xml"

    dfr = pdx.read_xml(url, ["estacoes"])
    return dfr


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def tratar_dados(
    dfr: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    mode: str = "dev",
) -> Tuple[pd.DataFrame, bool]:
    """
    Faz o request e salva dados localmente
    """
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

    dfr = pdx.fully_flatten(dfr).drop(drop_cols, axis=1).rename(rename_cols, axis=1)

    # Converte de UTC para horário São Paulo
    date_format = "%Y-%m-%d %H:%M:%S"
    dfr["data_medicao_utc"] = pd.to_datetime(dfr["data_medicao_utc"])
    dfr["data_medicao"] = (
        dfr["data_medicao_utc"]
        .dt.tz_convert("America/Sao_Paulo")
        .dt.strftime(date_format)
    )
    dfr["data_medicao"] = pd.to_datetime(dfr["data_medicao"])

    dfr = dfr.drop(["data_medicao_utc"], axis=1)

    # Converte variáveis que deveriam ser float para float
    float_cols = [
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
        "acumulado_chuva_mes",
    ]
    dfr[float_cols] = dfr[float_cols].apply(pd.to_numeric, errors="coerce")

    dfr = save_updated_rows_on_redis(
        dfr,
        dataset_id,
        table_id,
        unique_id="id_estacao",
        date_column="data_medicao",
        date_format=date_format,
        mode=mode,
    )

    # If df is empty stop flow
    empty_data = dfr.shape[0] == 0
    log(f"[DEBUG]: dataframe is empty: {empty_data}")

    return dfr, empty_data


@task
def salvar_dados(dfr: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    # Ordenação de variáveis
    cols_order = [
        "id_estacao",
        "data_medicao",
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
        "acumulado_chuva_mes",
    ]

    dfr = dfr[cols_order]

    prepath = Path("/tmp/precipitacao_websirene/")
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data_medicao"
    dataframe, partitions = parse_date_columns(dfr, partition_column)
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
