# -*- coding: utf-8 -*-
"""
Tasks for precipitacao_alertario
"""
from datetime import timedelta
import os
from pathlib import Path
from typing import Union, Tuple

import pandas as pd
import pandas_read_xml as pdx
import pendulum
from prefect import task

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
    dfr["data_medicao"] = pd.to_datetime(dfr["data_medicao"])

    # Cria coluna com data e hora da medição
    dfr["data"] = dfr["data_medicao"].dt.strftime("%Y-%m-%d")
    dfr["hora"] = dfr["data_medicao"].dt.strftime("%H")

    dfr = dfr.drop(["data_medicao_utc", "data_medicao"], axis=1)

    # Ordenação de variáveis
    cols_order = [
        "data",
        "hora",
        "id_estacao",
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


@task(nout=2)
def salvar_dados(dfr: pd.DataFrame) -> Tuple[Union[str, Path], str]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    # Pegar o dia máximo que aparece na base como partição
    max_date = str(dfr["data"].max())
    ano = max_date[:4]
    mes = str(int(max_date[5:7]))
    dia = str(int(max_date[8:10]))
    hora = str(dfr["hora"].max())

    partitions = f"ano={ano}/mes={mes}/dia={dia}"

    base_path = os.path.join(
        os.getcwd(), "data", "precipitacao_websirene", "output", partitions
    )

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    filename = os.path.join(base_path, f"dados_{max_date}_{hora}.csv")

    print(f"Saving {filename}")
    dfr.to_csv(filename, index=False)
    return filename, partitions
