# -*- coding: utf-8 -*-
"""
Tasks for comando
"""

import json
import os
from pathlib import Path
from typing import Any, Union, Tuple

import pandas as pd
import pendulum
from prefect import task
from prefect.triggers import all_successful

from pipelines.rj_cor.comando.eventos.utils import get_token, get_url, build_redis_key
from pipelines.utils.utils import get_redis_client, log


@task(nout=2)
def get_interval_on_redis(
    dataset_id: str, table_id: str, mode: str = "prod"
) -> Tuple[dict, str]:
    """
    Get the interval of data from Redis.
    """
    redis_client = get_redis_client()

    key = build_redis_key(dataset_id, table_id, mode)

    current_time = pendulum.now("America/Sao_Paulo")

    last_update = redis_client.get(key)
    if last_update is None:
        # Set to current_time - 30 days
        last_update = current_time.subtract(days=30).strftime("%Y-%m-%d %H:%M:%S.0")

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
def get_date_interval_from_string(date_interval_text: str) -> Tuple[dict, str]:
    """
    Get the date interval from a string.
    """
    date_interval = json.loads(date_interval_text)
    current_time = date_interval["fim"]
    return date_interval, current_time


@task(nout=2)
# pylint: disable=W0613
def download(date_interval, wait=None) -> Tuple[pd.DataFrame, str]:
    """
    Faz o request dos dados de eventos e das atividades do evento
    """

    auth_token = get_token()

    url_eventos = "http://ws.status.rio/statuscomando/v2/listarEventos"
    url_atividades_evento = (
        "http://ws.status.rio/statuscomando/v2/listarAtividadesDoEvento"
    )

    # Request Eventos
    response = get_url(url=url_eventos, parameters=date_interval, token=auth_token)

    eventos = pd.DataFrame(response["eventos"])

    rename_columns = {"id": "evento_id", "titulo": "pop_titulo"}

    eventos.rename(rename_columns, inplace=True, axis=1)
    log(f">>>>>>> eventos {eventos.head()}")
    eventos["evento_id"] = eventos["evento_id"].astype("int")

    evento_id_list = eventos["evento_id"].unique()

    atividades_evento = []
    problema_ids = []

    # Request AtividadesDoEvento
    for i in evento_id_list:
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
def not_none(something: Any) -> bool:
    """
    Returns True if something is not None.
    """
    return something is not None
