# -*- coding: utf-8 -*-
# pylint: disable=R0914,W0613,W0102,W0613,R0912,R0915,E1136,E1137,W0702
# flake8: noqa: E722
"""
Tasks for comando
"""

# from copy import deepcopy
from datetime import timedelta
import os
from pathlib import Path
from typing import Any, Union, Tuple
from uuid import uuid4
from unidecode import unidecode

import pandas as pd
import pendulum
from prefect import task

from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped

# from prefect.triggers import all_successful

from pipelines.rj_cor.comando.eventos.utils import (
    build_redis_key,
    compare_actual_df_with_redis_df,
    get_redis_output,  # TODO: atualizar o do utils.utils
    format_date,
    treat_wrong_id_pop,
)
from pipelines.utils.utils import (
    get_vault_secret,
    log,
    parse_date_columns,
    to_partitions,
)


@task
def get_date_interval(first_date, last_date) -> Tuple[dict, str]:
    """
    If `first_date` and `last_date` are provided, format it to DD/MM/YYYY. Else,
    get data from last 3 days.
    first_date: str YYYY-MM-DD
    last_date: str YYYY-MM-DD
    """
    if first_date and last_date:
        first_date, last_date = format_date(first_date, last_date)
    else:
        last_date = pendulum.today(tz="America/Sao_Paulo").date()
        first_date = last_date.subtract(days=1)  # atenção mudar para 3
        first_date, last_date = format_date(
            first_date.strftime("%Y-%m-%d"), last_date.strftime("%Y-%m-%d")
        )
    return first_date, last_date


@task
def get_redis_df(
    dataset_id: str,
    table_id: str,
    name: str,
    mode: str = "prod",
) -> pd.DataFrame:
    """
    Acess redis to get the last saved df and compare to actual df,
    return only the rows from actual df that are not already saved.
    """
    redis_key = build_redis_key(dataset_id, table_id, name, mode)
    log(f"Acessing redis_key: {redis_key}")

    dfr_redis = get_redis_output(redis_key, is_df=True)
    log(f"Redis output: {dfr_redis}")

    # if len(dfr_redis) == 0:
    #     dfr_redis = pd.DataFrame()
    #     dict_redis = {k: None for k in columns}
    #     print(f"\nCreating Redis fake values for key: {redis_key}\n")
    #     print(dict_redis)
    #     dfr_redis = pd.DataFrame(
    #         dict_redis, index=[0]
    #     )
    # else:
    #     dfr_redis = pd.DataFrame(
    #         dict_redis.items(), columns=columns
    #         )
    # print(f"\nConverting redis dict to df: \n{dfr_redis.head()}")

    return dfr_redis


@task(
    nout=1,
    max_retries=3,
    retry_delay=timedelta(seconds=60),
)
def download_data_ocorrencias(first_date, last_date, wait=None) -> pd.DataFrame:
    """
    Download data from API
    """
    # auth_token = get_token()

    url_secret = get_vault_secret("comando")["data"]
    url_eventos = url_secret["endpoint_eventos"]

    dfr = pd.read_json(f"{url_eventos}/?data_i={first_date}&data_f={last_date}")

    return dfr


@task(nout=2)
def treat_data_ocorrencias(
    dfr: pd.DataFrame,
    dfr_redis: pd.DataFrame,
    columns: list,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Rename cols and normalize data.
    """

    log("Start treating data")
    dfr = dfr.rename(
        columns={
            "id": "id_evento",
            "pop_id": "id_pop",
            "inicio": "data_inicio",
            "pop": "pop_titulo",
            "titulo": "pop_especificacao",
        }
    )

    dfr["id_evento"] = dfr["id_evento"].astype(float).astype(int).astype(str)

    log(f"Dataframe before comparing with last data saved on redis \n{dfr.head()}")
    columns = ["id_evento", "data_inicio", "status"]
    dfr, dfr_redis = compare_actual_df_with_redis_df(
        dfr,
        dfr_redis,
        columns,
    )
    log(f"Dataframe after comparing with last data saved on redis {dfr.head()}")

    # If df is empty stop flow
    if dfr.shape[0] == 0:
        skip_text = "No new data available on API"
        print(skip_text)
        raise ENDRUN(state=Skipped(skip_text))

    dfr["tipo"] = dfr["tipo"].replace(
        {
            "Primária": "Primario",
            "Secundária": "Secundario",
        }
    )
    dfr["descricao"] = dfr["descricao"].apply(unidecode)

    mandatory_cols = [
        "id_pop",
        "id_evento",
        "bairro",
        "data_inicio",
        "data_fim",
        "prazo",
        "descricao",
        "gravidade",
        "latitude",
        "longitude",
        "status",
        "tipo",
    ]
    # Create cols if they don exist on new API
    for col in mandatory_cols:
        if col not in dfr.columns:
            dfr[col] = None

    categorical_cols = [
        "bairro",
        "descricao",
        "gravidade",
        "status",
        "tipo",
        "pop_titulo",
    ]
    for i in categorical_cols:
        dfr[i] = dfr[i].str.capitalize()

    # This treatment is temporary. Now the id_pop from API is comming with the same value as id_evento
    dfr = treat_wrong_id_pop(dfr)
    log(f"This id_pop are missing {dfr[dfr.id_pop.isna()]} they were replaced by 99")
    dfr["id_pop"] = dfr["id_pop"].fillna(99)

    # Treat id_pop col
    dfr["id_pop"] = dfr["id_pop"].astype(float).astype(int)

    for col in ["data_inicio", "data_fim"]:
        dfr[col] = pd.to_datetime(dfr[col], errors="coerce")

    for col in ["data_inicio", "data_fim"]:
        dfr[col] = dfr[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Set the order to match the original table
    dfr = dfr[mandatory_cols]

    # Create a column with time of row creation to keep last event on dbt
    dfr["created_at"] = pendulum.now(tz="America/Sao_Paulo").strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    return dfr.drop_duplicates(), dfr_redis


@task(
    nout=1,
    max_retries=3,
    retry_delay=timedelta(seconds=60),
)
def download_data_atividades(first_date, last_date, wait=None) -> pd.DataFrame:
    """
    Download data from API
    """

    url_secret = get_vault_secret("comando")["data"]
    url_atividades_evento = url_secret["endpoint_atividades_evento"]

    dfr = pd.read_json(
        f"{url_atividades_evento}/?data_i={first_date}&data_f={last_date}"
    )

    return dfr


@task(nout=2)
def treat_data_atividades(
    dfr: pd.DataFrame,
    dfr_redis: pd.DataFrame,
    columns: list,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Normalize data to be similiar to old API.
    """

    print("Start treating data")
    dfr.orgao = dfr.orgao.replace(["\r", "\n"], ["", ""], regex=True)

    print(f"Dataframe before comparing with last data saved on redis {dfr.head()}")

    dfr, dfr_redis = compare_actual_df_with_redis_df(
        dfr,
        dfr_redis,
        columns,
    )
    print(f"Dataframe after comparing with last data saved on redis {dfr.head()}")

    # If df is empty stop flow
    if dfr.shape[0] == 0:
        skip_text = "No new data available on API"
        print(skip_text)
        raise ENDRUN(state=Skipped(skip_text))

    mandatory_cols = [
        "id_evento",
        "sigla",
        "orgao",  # esse não tem na tabela antiga
        "data_chegada",
        "data_inicio",
        "data_fim",
        "descricao",
        "status",
    ]

    # Create cols if they don exist on new API
    for col in mandatory_cols:
        if col not in dfr.columns:
            dfr[col] = None

    categorical_cols = [
        "sigla",
        "orgao",
        "descricao",
        "status",
    ]

    print("\n\nDEBUG", dfr[categorical_cols])
    for i in categorical_cols:
        dfr[i] = dfr[i].str.capitalize()
        # dfr[i] = dfr[i].apply(unidecode)

    for col in ["data_inicio", "data_fim", "data_chegada"]:
        dfr[col] = pd.to_datetime(dfr[col], errors="coerce")

    for col in ["data_inicio", "data_fim", "data_chegada"]:
        dfr[col] = dfr[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Set the order to match the original table
    dfr = dfr[mandatory_cols]

    # Create a column with time of row creation to keep last event on dbt
    dfr["created_at"] = pendulum.now(tz="America/Sao_Paulo").strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    return dfr.drop_duplicates(), dfr_redis


@task
def save_data(dataframe: pd.DataFrame) -> Union[str, Path]:
    """
    Save data on a csv file to be uploaded to GCP
    """

    prepath = Path("/tmp/data/")
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data_inicio"
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
def save_no_partition(dataframe: pd.DataFrame, append: bool = False) -> str:
    """
    Saves a dataframe to a temporary directory and returns the path to the directory.
    """

    if "sigla" in dataframe.columns:
        dataframe = dataframe.sort_values(["id_pop", "sigla", "acao"])
    else:
        dataframe = dataframe.sort_values("id_pop")

    path_to_directory = "/tmp/" + str(uuid4().hex) + "/"
    os.makedirs(path_to_directory, exist_ok=True)
    if append:
        current_time = pendulum.now("America/Sao_Paulo").strftime("%Y-%m-%d %H:%M:%S")
        dataframe.to_csv(path_to_directory + f"dados_{current_time}.csv", index=False)
    else:
        dataframe.to_csv(path_to_directory + "dados.csv", index=False)
    return path_to_directory


@task
def not_none(something: Any) -> bool:
    """
    Returns True if something is not None.
    """
    return something is not None
