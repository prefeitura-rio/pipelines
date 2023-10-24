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
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped

# from prefect import context

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_cemaden.utils import (
    parse_date_columns,
)
from pipelines.utils.utils import (
    log,
    to_partitions,
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
        "nomeestacao",
        "tipoestacao",
        "status",
    ]
    rename_cols = {
        "idestacao": "id_estacao",
        "ultimovalor": "acumulado_chuva_10_min",
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
    dados.replace(["-", np.nan], [0, None], inplace=True)

    # Altera valores negativos para None
    float_cols = [
        "acumulado_chuva_10_min",
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

    # Ajustando dados da meia-noite que vem sem o horário
    for index, row in dados.iterrows():
        try:
            date = pd.to_datetime(row["data_medicao"], format="%Y-%m-%d %H:%M:%S")
        except ValueError:
            date = pd.to_datetime(row["data_medicao"]) + pd.DateOffset(hours=0)

        dados.at[index, "data_medicao"] = date.strftime("%Y-%m-%d %H:%M:%S")

    log(f"Dataframe before comparing with last data saved on redis {dados.head()}")

    dados = save_updated_rows_on_redis(
        dados,
        dataset_id,
        table_id,
        unique_id="id_estacao",
        date_column="data_medicao",
        date_format=date_format,
        mode=mode,
    )

    log(f"Dataframe after comparing with last data saved on redis {dados.head()}")

    # If df is empty stop flow
    if dados.shape[0] == 0:
        skip_text = "No new data available on API"
        log(skip_text)
        raise ENDRUN(state=Skipped(skip_text))

    # Fixar ordem das colunas
    dados = dados[
        [
            "id_estacao",
            "data_medicao",
            "acumulado_chuva_10_min",
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

    return dados


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
