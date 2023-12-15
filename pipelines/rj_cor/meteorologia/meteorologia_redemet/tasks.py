# -*- coding: utf-8 -*-
"""
Tasks for meteorologia_redemet
"""
from datetime import timedelta
import json
from pathlib import Path
from typing import Tuple, Union
from unidecode import unidecode

import pandas as pd
import pendulum
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Failed
import requests

from pipelines.constants import constants
from pipelines.utils.utils import (
    get_vault_secret,
    log,
    to_partitions,
    parse_date_columns,
)


@task(nout=3)
def get_dates(first_date: str, last_date: str) -> Tuple[str, str]:
    """
    Task to get first and last date.
    If none date is passed on parameters or we are not doing a backfill
    the first_date will be yesterday and last_date will be today.
    Otherwise, this function will return date inputed on flow's parameters.
    """
    # a API sempre retorna o dado em UTC
    if first_date:
        backfill = 1
    else:
        last_date = pendulum.now("UTC").format("YYYY-MM-DD")
        first_date = pendulum.yesterday("UTC").format("YYYY-MM-DD")
        backfill = 0
    log(f"Selected first_date as: {first_date} and last_date as: {last_date}")

    return first_date, last_date, backfill


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_data(first_date: str, last_date: str) -> pd.DataFrame:
    """
    Request data from especified date range
    """

    # Stations inside Rio de Janeiro city
    rj_stations = [
        "SBAF",
        "SBGL",
        "SBJR",
        "SBRJ",
        "SBSC",
    ]

    redemet_token = get_vault_secret("redemet-token")

    # Converte datas em int para cálculo de faixas.
    first_date_int = int(first_date.replace("-", ""))
    last_date_int = int(last_date.replace("-", ""))

    raw = []
    for id_estacao in rj_stations:
        base_url = f"https://api-redemet.decea.mil.br/aerodromos/info?api_key={redemet_token['data']['token']}"  # noqa
        for data in range(first_date_int, last_date_int + 1):
            for hora in range(24):
                url = f"{base_url}&localidade={id_estacao}&datahora={data:06}{hora:02}"
                res = requests.get(url)
                if res.status_code != 200:
                    log(f"Problema no id: {id_estacao}, {res.status_code}, {url}")
                    continue
                res_data = json.loads(res.text)
                if res_data["status"] is not True:
                    log(f"Problema no id: {id_estacao}, {res_data['message']}, {url}")
                    continue
                if "data" not in res_data["data"]:
                    # Sem dataframe para esse horario
                    continue
                raw.append(res_data)

    # Extrai objetos de dataframe
    raw = [res_data["data"] for res_data in raw]

    # converte para dataframe
    dataframe = pd.DataFrame(raw)

    return dataframe


@task
def treat_data(dataframe: pd.DataFrame, backfill: bool = 0) -> pd.DataFrame:
    """
    Rename cols, convert timestamp, filter data for the actual date
    """

    drop_cols = ["nome", "cidade", "lon", "lat", "localizacao", "tempoImagem", "metar"]
    # Check if all columns are on the dataframe
    drop_cols = [c for c in drop_cols if c in dataframe.columns]

    # Remove columns that are already in another table
    dataframe = dataframe.drop(drop_cols, axis=1)

    rename_cols = {
        "localidade": "id_estacao",
        "ur": "umidade",
    }

    dataframe = dataframe.rename(columns=rename_cols)

    # Convert UTC time to America/Sao Paulo
    formato = "DD/MM/YYYY HH:mm(z)"
    dataframe["data"] = dataframe["data"].apply(
        lambda x: pendulum.from_format(x, formato)
        .in_tz("America/Sao_Paulo")
        .format(formato)
    )

    # Order variables
    primary_keys = ["id_estacao", "data"]
    other_cols = [c for c in dataframe.columns if c not in primary_keys]

    dataframe = dataframe[primary_keys + other_cols]

    # Clean data
    dataframe["temperatura"] = dataframe["temperatura"].apply(
        lambda x: None if x[:-2] == "NIL" else int(x[:-2])
    )
    dataframe["umidade"] = dataframe["umidade"].apply(
        lambda x: None if "%" not in x else int(x[:-1])
    )

    dataframe["data"] = pd.to_datetime(dataframe.data, format="%d/%m/%Y %H:%M(%Z)")

    # Define colunas que serão salvas
    dataframe = dataframe[
        [
            "id_estacao",
            "data",
            "temperatura",
            "umidade",
            "condicoes_tempo",
            "ceu",
            "teto",
            "visibilidade",
        ]
    ]

    dataframe = dataframe.drop_duplicates(subset=["id_estacao", "data"])

    log(f"Dados antes do filtro dia:\n{dataframe[['id_estacao', 'data']]}")

    if not backfill:
        # Select our date
        br_timezone = pendulum.now("America/Sao_Paulo").format("YYYY-MM-DD")

        # Select only data from that date
        dataframe = dataframe[dataframe["data"].dt.date.astype(str) == br_timezone]

    log(f">>>> min hora {dataframe[~dataframe.temperatura.isna()].data.min()}")
    log(f">>>> max hora {dataframe[~dataframe.temperatura.isna()].data.max()}")

    dataframe["data"] = dataframe["data"].dt.strftime("%Y-%m-%d %H:%M:%S")
    dataframe.rename(columns={"data": "data_medicao"}, inplace=True)

    dataframe["ceu"] = dataframe["ceu"].str.capitalize()

    return dataframe


@task
def save_data(
    dataframe: pd.DataFrame, partition_column: str = "data_medicao"
) -> Union[str, Path]:
    """
    Salve dataframe as a csv file
    """

    prepath = Path("/tmp/meteorologia_redemet/")
    prepath.mkdir(parents=True, exist_ok=True)

    dataframe, partitions = parse_date_columns(dataframe, partition_column)

    # Cria partições a partir da data
    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
    )
    log(f"[DEBUG] Files saved on {prepath}")
    return prepath


@task
def download_stations_data() -> pd.DataFrame:
    """
    Download station information
    """

    redemet_token = get_vault_secret("redemet-token")
    base_url = (
        f"https://api-redemet.decea.mil.br/aerodromos/?api_key={redemet_token}"  # noqa
    )
    url = f"{base_url}&pais=Brasil"
    res = requests.get(url)
    if res.status_code != 200:
        print(f"Problem on request: {res.status_code}, {url}")
    res_data = json.loads(res.text)

    dataframe = pd.DataFrame(res_data["data"])
    return dataframe


@task
def treat_stations_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Treat station data
    """
    rename_cols = {
        "lat_dec": "latitude",
        "lon_dec": "longitude",
        "nome": "estacao",
        "altitude_metros": "altitude",
        "cod": "id_estacao",
    }
    dataframe = dataframe.rename(rename_cols, axis=1)

    dataframe = dataframe[dataframe.cidade.str.contains("Rio de Janeiro")]

    dataframe["estacao"] = dataframe["estacao"].apply(unidecode)
    dataframe["data_atualizacao"] = pendulum.now(tz="America/Sao_Paulo").format(
        "YYYY-MM-DD"
    )

    keep_cols = [
        "id_estacao",
        "estacao",
        "latitude",
        "longitude",
        "altitude",
        "data_atualizacao",
    ]
    return dataframe[keep_cols]


@task
def check_for_new_stations(dataframe: pd.DataFrame):
    """
    Check if the updated stations are the same as before.
    If not, consider flow as failed and call attention to
    change treat_data task.
    """

    stations_before = [
        "SBAF",
        "SBGL",
        "SBJR",
        "SBRJ",
        "SBSC",
    ]
    new_stations = [
        i for i in dataframe.id_estacao.unique() if i not in stations_before
    ]
    if len(new_stations) != 0:
        message = f"New station identified. You need to update REDEMET\
              flow and add station(s) {new_stations}"
        log(message)
        raise ENDRUN(state=Failed(message))
