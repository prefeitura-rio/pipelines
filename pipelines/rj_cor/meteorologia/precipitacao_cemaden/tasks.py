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
from prefect.engine.state import Skipped  # , Failed
from pipelines.constants import constants
from pipelines.utils.utils import (
    log,
    parse_date_columns,
    save_updated_rows_on_redis,
    to_partitions,
)


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_data() -> pd.DataFrame:
    """
    Download data from API
    """

    url = "http://sjc.salvar.cemaden.gov.br/resources/graficos/interativo/getJson2.php?uf=RJ"
    dataframe = pd.read_json(url)
    return dataframe


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treat_data(
    dataframe: pd.DataFrame, dataset_id: str, table_id: str, mode: str = "dev"
) -> Tuple[pd.DataFrame, bool]:
    """
    Rename cols and filter data using hour and minute from the nearest current timestamp
    """

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

    dataframe = (
        dataframe[(dataframe["codibge"] == 3304557) & (dataframe["tipoestacao"] == 1)]
        .drop(drop_cols, axis=1)
        .rename(rename_cols, axis=1)
    )
    log(f"\n[DEBUG]: df.head() {dataframe.head()}")

    # Convert from UTC to São Paulo timezone
    dataframe["data_medicao_utc"] = pd.to_datetime(
        dataframe["data_medicao_utc"], dayfirst=True
    ) + pd.DateOffset(hours=0)
    dataframe["data_medicao"] = (
        dataframe["data_medicao_utc"]
        .dt.tz_localize("UTC")
        .dt.tz_convert("America/Sao_Paulo")
    )
    see_cols = ["data_medicao_utc", "data_medicao", "id_estacao", "acumulado_chuva_1_h"]
    log(f"DEBUG: data utc -> GMT-3 {dataframe[see_cols]}")

    date_format = "%Y-%m-%d %H:%M:%S"
    dataframe["data_medicao"] = dataframe["data_medicao"].dt.strftime(date_format)

    log(f"DEBUG: data {dataframe[see_cols]}")

    # Change values '-' and np.nan to NULL
    dataframe.replace(["-", np.nan], [0, None], inplace=True)

    # Change negative values to None
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
    dataframe[float_cols] = np.where(
        dataframe[float_cols] < 0, None, dataframe[float_cols]
    )

    # Eliminate where the id_estacao is the same keeping the smallest one
    dataframe.sort_values(["id_estacao", "data_medicao"] + float_cols, inplace=True)
    dataframe.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    log(f"Dataframe before comparing with last data saved on redis {dataframe.head()}")

    dataframe = save_updated_rows_on_redis(
        dataframe,
        dataset_id,
        table_id,
        unique_id="id_estacao",
        date_column="data_medicao",
        date_format=date_format,
        mode=mode,
    )

    log(f"Dataframe after comparing with last data saved on redis {dataframe.head()}")

    # If df is empty stop flow
    if dataframe.shape[0] == 0:
        skip_text = "No new data available on API"
        log(skip_text)
        raise ENDRUN(state=Skipped(skip_text))

    # Fix columns order
    dataframe = dataframe[
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

    return dataframe


@task
def save_data(dataframe: pd.DataFrame) -> Union[str, Path]:
    """
    Save data on a csv file to be uploaded to GCP
    """

    prepath = Path("/tmp/precipitacao_cemaden/")
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data_medicao"
    dataframe, partitions = parse_date_columns(dataframe, partition_column)
    current_time = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")

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
def check_for_new_stations(
    dataframe: pd.DataFrame,
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Check if the updated stations are the same as before.
    If not, consider flow as failed and call attention to
    add this new station on estacoes_redemet.
    I can't automatically update this new station, because
    I couldn't find a url that gives me the lat and lon for
    all the stations. To manually update enter
    http://www2.cemaden.gov.br/mapainterativo/# >
    Download de Dados > Estações Pluviométricas and fill the
    requested information.
    """

    stations_before = [
        "Abolicao",
        "Tanque jacarepagua",
        "Penha",
        "Praca seca",
        "Gloria",
        "Est. pedra bonita",
        "Jardim maravilha",
        "Santa cruz",
        "Realengo batan",
        "Padre miguel",
        "Salgueiro",
        "Andarai",
        "Ciep samuel wainer",
        "Vargem pequena",
        "Jacarepagua",
        "Ciep dr. joao ramos de souza",
        "Sao conrado",
        "Catete",
        "Pavuna",
        "Vigario geral",
        "Defesa civil santa cruz",
        "Vicente de carvalho",
        "Alto da boa vista",
        "Tijuca",
        "Usina",
        "Higienopolis",
        "Pilares",
        "Ilha de paqueta",
    ]
    new_stations = [
        i
        for i in dataframe.id_estacao.unique()
        if i.capitalize() not in stations_before
    ]
    if len(new_stations) != 0:
        message = f"New station identified. You need to update CEMADEN\
              estacoes_cemaden adding station(s) {new_stations}"
        log(message)
        # raise ENDRUN(state=Failed(message))
