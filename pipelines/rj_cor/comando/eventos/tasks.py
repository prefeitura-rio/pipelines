# -*- coding: utf-8 -*-
"""
Tasks for comando
"""

from copy import deepcopy
import json
import os
from pathlib import Path
from typing import Any, Union, Tuple
from uuid import uuid4

import pandas as pd
import pendulum
from prefect import task
from prefect.triggers import all_successful

from pipelines.rj_cor.comando.eventos.utils import get_token, get_url, build_redis_key
from pipelines.utils.utils import get_redis_client, get_vault_secret, log


@task(nout=2)
def get_date_interval(
    date_interval_text: str,
    dataset_id: str,
    table_id: str,
    mode: str = "prod",
) -> Tuple[dict, str]:
    """
    If `date_interval_text` is provided, parse it for the date interval. Else,
    get the date interval from Redis.
    """
    if date_interval_text:
        log(f">>>>>>>>>>> Date interval was provided: {date_interval_text}")
        date_interval = json.loads(date_interval_text)
        current_time = date_interval["fim"]
        log(f">>>>>>>>>>> Date interval: {date_interval}")
        return date_interval, current_time

    log(">>>>>>>>>>> Date interval was not provided")
    redis_client = get_redis_client()

    key = build_redis_key(dataset_id, table_id, mode)

    current_time = pendulum.now("America/Sao_Paulo")

    last_update = redis_client.get(key)
    if last_update is None:
        log("Last update was not found in Redis, setting it to D-30")
        # Set to current_time - 30 days
        last_update = current_time.subtract(days=30).strftime("%Y-%m-%d %H:%M:%S.0")
    log(f">>>>>>>>>>> Last update: {last_update}")

    current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S.0")

    date_interval = {
        "inicio": last_update,
        "fim": current_time_str,
    }

    log(f">>>>>>> date_interval: {date_interval}")

    return date_interval, current_time_str


@task(trigger=all_successful)
def set_last_updated_on_redis(
    dataset_id: str, table_id: str, mode: str = "prod", current_time: str = None
) -> None:
    """
    Set the last updated time on Redis.
    """
    redis_client = get_redis_client()

    key = build_redis_key(dataset_id, table_id, mode)

    if not current_time:
        current_time = pendulum.now("America/Sao_Paulo").strftime("%Y-%m-%d %H:%M:%S.0")

    redis_client.set(key, current_time)


@task(nout=2)
# pylint: disable=W0613
def download_eventos(date_interval, wait=None) -> Tuple[pd.DataFrame, str]:
    """
    Faz o request dos dados de eventos e das atividades do evento
    """

    auth_token = get_token()

    url_secret = get_vault_secret("comando")["data"]
    url_eventos = url_secret["endpoint_eventos"]
    url_atividades_evento = url_secret["endpoint_atividades_evento"]

    # Request Eventos
    response = get_url(url=url_eventos, parameters=date_interval, token=auth_token)

    eventos = pd.DataFrame(response["eventos"])

    rename_columns = {"id": "evento_id", "titulo": "pop_titulo"}

    eventos.rename(rename_columns, inplace=True, axis=1)
    log(f">>>>>>> eventos\n{eventos.head()}")
    eventos["evento_id"] = eventos["evento_id"].astype("int")

    evento_id_list = eventos["evento_id"].unique()
    log(f">>>>>>> evento_id_list: {evento_id_list}")

    atividades_evento = []
    problema_ids = []

    # Request AtividadesDoEvento
    for i in evento_id_list:
        log(f">>>>>>> Requesting AtividadesDoEvento for evento_id: {i}")
        response = get_url(url=url_atividades_evento + f"?eventoId={i}")
        if "atividades" in response.keys():
            response = response["atividades"]
            for elem in response:
                elem["evento_id"] = i
                atividades_evento.append(elem)
        else:
            problema_ids.append(i)

    atividades_evento = pd.DataFrame(atividades_evento)
    log(f">>>>>>> atv eventos {atividades_evento.head()}")

    # Fixa colunas e ordem
    eventos = eventos[
        [
            "pop_id",
            "bairro",
            "latitude",
            "inicio",
            "pop_titulo",
            "fim",
            "prazo",
            "descricao",
            "informe_id",
            "gravidade",
            "evento_id",
            "longitude",
            "status",
        ]
    ]
    atividades_evento = atividades_evento[
        [
            "orgao",
            "chegada",
            "inicio",
            "nome",
            "fim",
            "descricao",
            "status",
            "evento_id",
        ]
    ]

    return eventos, atividades_evento


@task
def get_pops() -> pd.DataFrame:
    """
    Get the list of POPS from the API
    """
    log(">>>>>>> Requesting POPS")

    auth_token = get_token()

    url_secret = get_vault_secret("comando")["data"]
    url = url_secret["endpoint_pops"]

    response = get_url(url=url, token=auth_token)

    pops = pd.DataFrame(response["objeto"])
    pops["pop_id"] = pops["pop_id"].astype("int")

    log(f">>>>>>> pops\n{pops.head()}")

    return pops


@task
def get_atividades_pops(pops: pd.DataFrame) -> pd.DataFrame:
    """
    Get the list of POP's activities from API
    """
    log(">>>>>>> Requesting POP's activities")

    auth_token = get_token()

    url_secret = get_vault_secret("comando")["data"]
    url = url_secret["endpoint_atividades_pop"]

    pop_ids = pops["id"].unique()

    atividades_pops = []
    for pop_id in pop_ids:
        log(f">>>>>>> Requesting POP's activities for pop_id: {pop_id}")
        response = get_url(url=url + f"?popId={pop_id}", token=auth_token)
        row_template = {
            "pop": response["pop"],
            "sigla": "",
            "orgao": "",
            "acao": "",
        }
        for activity in response["atividades"]:
            row = deepcopy(row_template)
            row["sigla"] = activity["sigla"]
            row["orgao"] = activity["orgao"]
            row["acao"] = activity["acao"]
            atividades_pops.append(row)

    dataframe = pd.DataFrame(atividades_pops)
    log(f">>>>>>> atividades_pops\n{dataframe.head()}")

    return dataframe


@task
def salvar_dados(dfr: pd.DataFrame, current_time: str, name: str) -> Union[str, Path]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    ano = current_time[:4]
    mes = str(int(current_time[5:7]))
    data = str(current_time[:10])
    partitions = os.path.join(
        f"ano_particao={ano}", f"mes_particao={mes}", f"data_particao={data}"
    )

    base_path = os.path.join(os.getcwd(), "data", "comando", name)

    partition_path = os.path.join(base_path, partitions)

    if not os.path.exists(partition_path):
        os.makedirs(partition_path)

    df_path = os.path.join(partition_path, f"dados_{current_time}.csv")

    dfr.to_csv(df_path, index=False)
    log(f">>>>>>> base_path {base_path}")
    return base_path


@task
def save_no_partition(dataframe: pd.DataFrame) -> str:
    """
    Saves a dataframe to a temporary directory and returns the path to the directory.
    """
    path_to_directory = "/tmp/" + str(uuid4().hex) + "/"
    os.makedirs(path_to_directory, exist_ok=True)
    dataframe.to_csv(path_to_directory + "dados.csv", index=False)
    return path_to_directory


@task
def not_none(something: Any) -> bool:
    """
    Returns True if something is not None.
    """
    return something is not None
