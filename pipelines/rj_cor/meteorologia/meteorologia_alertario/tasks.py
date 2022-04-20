# -*- coding: utf-8 -*-
"""
Tasks for meteorologia_inmet
"""
from datetime import datetime
import os
from pathlib import Path
from typing import Tuple, Union

import numpy as np
import pandas as pd
import pandas_read_xml as pdx
import pendulum
from prefect import task
import requests


@task
def download_tratar_dados() -> pd.DataFrame:
    """
    Faz o request na data especificada e retorna dados
    """
    url = "http://alertario.rio.rj.gov.br/upload/xml/Chuvas.xml"

    # verificar se ele manteve certinho o id da estação

    drop_cols = [
        "@hora",
        "estacao|@nome",
        "estacao|localizacao|@bacia",
        "estacao|localizacao|@latitude",
        "estacao|localizacao|@longitude",
    ]
    rename_cols = {
        "estacao|@id": "id_estacao",
        "estacao|@type": "tipo_estacao",
        "estacao|chuvas|@h01": "acumulado_chuva_1_h",
        "estacao|chuvas|@h04": "acumulado_chuva_4_h",
        "estacao|chuvas|@h24": "acumulado_chuva_24_h",
        "estacao|chuvas|@h96": "acumulado_chuva_96_h",
        "estacao|chuvas|@hora": "data_medicao",
        "estacao|chuvas|@m15": "acumulado_chuva_15_min",
        "estacao|chuvas|@mes": "acumulado_chuva_mes",
        "estacao|met|@dirvento": "direcao_vento",
        "estacao|met|@pressao": "pressao",
        "estacao|met|@temperatura": "temperatura",
        "estacao|met|@umidade": "umidade",
        "estacao|met|@velvento": "velocidade_vento",
    }

    dfr = pdx.read_xml(url, ["estacoes"])
    dfr = pdx.fully_flatten(dfr).drop(drop_cols, axis=1).rename(rename_cols, axis=1)

    # Cria colunas de data e hora
    dfr["data_medicao"] = pd.to_datetime(dfr["data_medicao"])
    dfr["data"] = dfr["data_medicao"].dt.strftime("%Y-%m-%d")
    dfr["hora"] = dfr["data_medicao"].dt.strftime("%H")

    # df = df.drop(['data_medicao_utc', 'data_medicao'], axis=1)
    dfr = dfr.drop(["data_medicao"], axis=1)

    # Deixa apenas estações meteorológicas
    dfr = dfr[dfr["tipo_estacao"] == "met"].drop(["tipo_estacao"], axis=1)

    # Definir ordem das colunas
    cols_order = [
        "data",
        "hora",
        "id_estacao",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
        "acumulado_chuva_15_min",
        "acumulado_chuva_mes",
        "direcao_vento",
        "pressao",
        "temperatura",
        "umidade",
        "velocidade_vento",
    ]

    dfr = dfr[cols_order]

    dfr = dfr.replace("None", np.nan)

    # Converte variáveis que deveriam ser float para float
    float_cols = [
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
        "acumulado_chuva_15_min",
        "acumulado_chuva_mes",
        "direcao_vento",
        "pressao",
        "temperatura",
        "umidade",
        "velocidade_vento",
    ]
    dfr[float_cols] = dfr[float_cols].astype(float)

    dfr["horario"] = pd.to_datetime(dfr.horario, format="%H:%M:%S").dt.time
    dfr["data"] = pd.to_datetime(dfr.data, format="%Y-%m-%d")

    # Ordenamento de variáveis
    chaves_primarias = ["id_estacao", "data", "horario"]
    demais_cols = [c for c in dfr.columns if c not in chaves_primarias]
    dfr = dfr[chaves_primarias + demais_cols]

    return dfr


@task
def salvar_dados(dfr: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados em csv
    """
    base_path = os.path.join(os.getcwd(), "data", "meteorologia_alertario", "output")

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    current_time = pendulum.now()
    filename = os.path.join(base_path, f"dados_{current_time}.csv")

    print(f"Saving {filename}")
    dfr.to_csv(filename, index=False)
    return filename
