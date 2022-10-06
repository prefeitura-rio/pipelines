# -*- coding: utf-8 -*-
# pylint: disable=R0914,W0613,W0102
"""
Tasks for comando
"""

from copy import deepcopy
import json
import os
from pathlib import Path
import time
from typing import Any, Union, Tuple
from uuid import uuid4

import pandas as pd
import pendulum
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped
from prefect.triggers import all_successful

from pipelines.rj_cor.comando.eventos.utils import get_token, get_url, build_redis_key
from pipelines.utils.utils import get_redis_client, get_vault_secret, log, to_partitions


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

    key_last_update = build_redis_key(dataset_id, table_id, "last_update", mode)

    current_time = pendulum.now("America/Sao_Paulo")

    last_update = redis_client.get(key_last_update)
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

    log(f">>>>>>>>>>> date_interval: {date_interval}")

    return date_interval, current_time_str


@task(trigger=all_successful)
def set_last_updated_on_redis(
    dataset_id: str,
    table_id: str,
    mode: str = "prod",
    current_time: str = None,
    problem_ids_atividade: str = None,
) -> None:
    """
    Set the last updated time on Redis.
    """
    redis_client = get_redis_client()

    if not current_time:
        current_time = pendulum.now("America/Sao_Paulo").strftime("%Y-%m-%d %H:%M:%S.0")

    key_last_update = build_redis_key(dataset_id, table_id, "last_update", mode)
    redis_client.set(key_last_update, current_time)

    key_problema_ids = build_redis_key(dataset_id, table_id, "problema_ids", mode)

    problem_ids_atividade_antigos = redis_client.get(key_problema_ids)

    if problem_ids_atividade_antigos is not None:
        problem_ids_atividade = problem_ids_atividade + list(
            problem_ids_atividade_antigos
        )

    redis_client.set(key_problema_ids, list(set(problem_ids_atividade)))


@task(nout=3)
# pylint: disable=W0613,R0914,R0912,R0915
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

    if "eventos" in response and len(response["eventos"]) > 0:
        eventos = pd.DataFrame(response["eventos"])
    elif ("eventos" in response) and (len(response["eventos"])) == 0:
        log("No events found on this date interval")
        skip = Skipped("No events found on this date interval")
        raise ENDRUN(state=skip)
    else:
        raise Exception(f"{response}")

    rename_columns = {
        "id": "id_evento",
        "titulo": "pop_titulo",
        "pop_id": "id_pop",
        "inicio": "data_inicio",
        "fim": "data_fim",
    }

    eventos.rename(rename_columns, inplace=True, axis=1)
    eventos["id_evento"] = eventos["id_evento"].astype("int")
    evento_id_list = eventos["id_evento"].unique()

    atividades_evento = []
    problema_ids_atividade = []
    problema_ids_request = []

    # Request AtividadesDoEvento
    for i in evento_id_list:
        # log(f">>>>>>> Requesting AtividadesDoEvento for id_evento: {i}")
        try:
            response = get_url(
                url=url_atividades_evento + f"?eventoId={i}", token=auth_token
            )
            if "atividades" in response.keys():
                response = response["atividades"]
                for elem in response:
                    elem["id_evento"] = i
                    atividades_evento.append(elem)
            else:
                problema_ids_atividade.append(i)
        except Exception as exc:
            log(
                f"Request AtividadesDoEvento for id_evento: {i}"
                + f"resulted in the following error: {exc}"
            )
            problema_ids_request.append(i)
            raise exc
    log(f"\n>>>>>>> problema_ids_request: {problema_ids_request}")
    log(f"\n>>>>>>> problema_ids_atividade: {problema_ids_atividade}")

    problema_ids_atividade = problema_ids_atividade + problema_ids_request

    atividades_evento = pd.DataFrame(atividades_evento)
    atividades_evento.rename(
        {
            "orgao": "sigla",
            "titulo": "pop_titulo",
            "evento_id": "id_evento",
            "pop_id": "id_pop",
            "inicio": "data_inicio",
            "fim": "data_fim",
            "chegada": "data_chegada",
        },
        inplace=True,
        axis=1,
    )

    eventos_cols = [
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
    for col in eventos_cols:
        if col not in eventos.columns:
            eventos[col] = None

    atividades_evento_cols = [
        "id_evento",
        "sigla",
        "data_chegada",
        "data_inicio",
        "data_fim",
        "descricao",
        "status",
    ]
    for col in atividades_evento_cols:
        if col not in atividades_evento.columns:
            atividades_evento[col] = None

    eventos_categorical_cols = [
        "bairro",
        "prazo",
        "descricao",
        "gravidade",
        "status",
        "tipo",
    ]
    for i in eventos_categorical_cols:
        eventos[i] = eventos[i].str.capitalize()

    atividade_evento_categorical_cols = ["sigla", "descricao", "status"]
    for i in atividade_evento_categorical_cols:
        atividades_evento[i] = atividades_evento[i].str.capitalize()

    eventos_datas_cols = ["data_inicio", "data_fim"]
    atividades_eventos_datas_cols = ["data_chegada", "data_inicio", "data_fim"]
    eventos[eventos_datas_cols] = eventos[eventos_datas_cols].fillna("1970-01-01")
    atividades_evento[atividades_eventos_datas_cols] = atividades_evento[
        atividades_eventos_datas_cols
    ].fillna("1970-01-01")

    # Fixa colunas e ordem
    eventos = eventos[eventos_cols].drop_duplicates()
    atividades_evento = atividades_evento[atividades_evento_cols].drop_duplicates()
    return eventos, atividades_evento, problema_ids_atividade


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
    pops["id"] = pops["id"].astype("int")
    pops = pops.rename({"id": "id_pop", "titulo": "pop_titulo"}, axis=1)
    pops["pop_titulo"] = pops["pop_titulo"].str.capitalize()

    return pops[["id_pop", "pop_titulo"]]


@task(nout=2)
def get_atividades_pops(pops: pd.DataFrame, redis_pops: list) -> pd.DataFrame:
    """
    Get the list of POP's activities from API
    """
    log(">>>>>>> Requesting POP's activities")

    auth_token = get_token()

    url_secret = get_vault_secret("comando")["data"]
    url = url_secret["endpoint_atividades_pop"]

    pop_ids = pops["id_pop"].unique()

    atividades_pops = []
    for pop_id in pop_ids:
        log(f">>>>>>> Requesting POP's activities for pop_id: {pop_id}")
        response = get_url(url=url + f"?popId={pop_id}", token=auth_token)

        tentativa = 0
        while "error" in response.keys() and tentativa <= 5:
            log(
                f">>>>>>> Requesting POP's activities for pop_id: {pop_id} Time: {tentativa+1}"
            )
            time.sleep(60)
            response = get_url(url=url + f"?popId={pop_id}", token=auth_token)
            tentativa += 1

        if "error" in response.keys() and tentativa > 5:
            continue

        row_template = {
            "pop_titulo": response["pop"],
            "id_pop": pop_id,
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

    for i in ["sigla", "orgao", "acao"]:
        dataframe[i] = dataframe[i].str.capitalize()

    dataframe["key"] = (
        dataframe["id_pop"].astype(str)
        + "_"
        + dataframe["sigla"]
        + "_"
        + dataframe["acao"]
    )
    update_pops_redis = dataframe["key"].unique()

    # Checar pop_ids não salvos no redis
    update_pops_redis = [i for i in update_pops_redis if i not in redis_pops]

    if len(update_pops_redis) < 1:
        update_pops_redis = None
        dataframe = pd.DataFrame()
    else:
        # mantém apenas esses pop_ids no dataframe
        dataframe = dataframe[dataframe["key"].isin(update_pops_redis)]
        dataframe = dataframe[["id_pop", "sigla", "orgao", "acao"]]

    return dataframe, update_pops_redis


@task
def salvar_dados(dfr: pd.DataFrame, current_time: str, name: str) -> Union[str, Path]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    dfr["ano_particao"] = pd.to_datetime(dfr["data_inicio"]).dt.year
    dfr["mes_particao"] = pd.to_datetime(dfr["data_inicio"]).dt.month
    dfr["data_particao"] = pd.to_datetime(dfr["data_inicio"]).dt.date
    dfr["ano_particao"] = dfr["ano_particao"].astype("int")
    dfr["mes_particao"] = dfr["mes_particao"].astype("int")

    partitions_path = Path(os.getcwd(), "data", "comando", name)
    if not os.path.exists(partitions_path):
        os.makedirs(partitions_path)

    to_partitions(
        dfr,
        partition_columns=["ano_particao", "mes_particao", "data_particao"],
        savepath=partitions_path,
        suffix=current_time,
    )
    return partitions_path


@task
def save_no_partition(dataframe: pd.DataFrame, append: bool = False) -> str:
    """
    Saves a dataframe to a temporary directory and returns the path to the directory.
    """
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
