# -*- coding: utf-8 -*-
"""
Tasks for precipitacao_alertario
"""
from datetime import timedelta
import os
from pathlib import Path
from typing import Union

import pandas as pd
from prefect import task
import pandas_read_xml as pdx

from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_tratar_dados() -> pd.DataFrame:
    """
    Faz o request e salva dados localmente
    """

    # Acessar url websirene
    url = "http://websirene.rio.rj.gov.br/xml/chuvas.xml"

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

    dfr = pdx.read_xml(url, ["estacoes"])
    dfr = pdx.fully_flatten(dfr).drop(drop_cols, axis=1).rename(rename_cols, axis=1)

    # Converte de UTC para horário São Paulo
    dfr["data_medicao_utc"] = pd.to_datetime(dfr["data_medicao_utc"])
    dfr["data_medicao"] = (
        dfr["data_medicao_utc"]
        .dt.tz_convert("America/Sao_Paulo")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    dfr["data_medicao"] = pd.to_datetime(dfr["data"])

    dfr = dfr.drop(["data_medicao_utc"], axis=1)

    # Ordenação de variáveis
    cols_order = [
        "id_estacao",
        "data",
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
        "acumulado_chuva_mes",
    ]

    dfr = dfr[cols_order]

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

    return dfr


@task
def salvar_dados(dfr: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    # Pegar o dia máximo que aparece na base como partição
    max_date = str(dfr["data"].max())
    ano = max_date[:4]
    mes = str(int(max_date[5:7]))
    data = str(max_date[:10])

    partitions = os.path.join(
        f"ano_particao={ano}", f"mes_particao={mes}", f"data_particao={data}"
    )

    base_path = os.path.join(os.getcwd(), "data", "precipitacao_websirene", "output")

    partition_path = os.path.join(base_path, partitions)

    if not os.path.exists(partition_path):
        os.makedirs(partition_path)

    filename = os.path.join(partition_path, f"dados_{max_date}.csv")

    print(f"Saving {filename}")
    dfr.to_csv(filename, index=False)
    return base_path
