# -*- coding: utf-8 -*-
# pylint: disable=W0102, C0103
"""
Tasks for comando
"""

import os
from pathlib import Path
from typing import Union, Tuple

import pandas as pd
import pendulum
from prefect import task

from pipelines.rj_cor.comando.eventos.utils import get_url
from pipelines.utils.utils import get_redis_client, log


@task
def get_and_save_date_redis(dataset_id: str, table_id: str, mode: str = "prod") -> dict:
    """
    Acess redis to get the last time each id_estacao was updated, return
    updated stations as a DataFrame and save new dates on redis
    """
    redis_client = get_redis_client()

    key = "meio_ambiente_clima_staging.taxa_precipitacao_alertario"

    key = dataset_id + "." + table_id
    if mode == "dev":
        key = f"{mode}.{key}"

    # apagar próxima linha
    redis_client.set(key, "2022-07-03 16:00:00.0")

    # Access all data saved on redis with this key
    last_update = redis_client.get(key)  # .decode("utf-8")

    current_time = pendulum.now("America/Sao_Paulo").strftime(
        "%Y-%m-%d %H:%M:%S.0"
    )  # salvar no redis

    date_interval = {"inicio": last_update, "fim": current_time}
    log(f">>>>>>> date_interval: {date_interval}")

    # Save current_time on redis
    redis_client.set(key, current_time)

    return date_interval


@task(nout=2)
def download(date_interval) -> Tuple[pd.DataFrame, str]:
    """
    Faz o request dos dados de eventos e das atividades do evento
    """
    url_eventos = "http://ws.status.rio/statuscomando/v2/listarEventos"
    url_atividades_evento = (
        "http://ws.status.rio/statuscomando/v2/listarAtividadesDoEvento"
    )

    # Request Eventos
    response = get_url(url=url_eventos, parameters=date_interval)

    eventos = pd.DataFrame(response["eventos"])

    rename_columns = {"id": "evento_id", "titulo": "pop_titulo"}

    eventos.rename(rename_columns, inplace=True, axis=1)
    log(f">>>>>>> eventos {eventos.head()}")
    eventos["evento_id"] = eventos["evento_id"].astype("int")

    evento_id_list = eventos.evento_id.unique()

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
    eventos.name = "eventos"
    atividades_evento.name = "atividades_evento"

    return eventos, atividades_evento


@task
def salvar_dados(dfr: pd.DataFrame, current_time: str) -> Union[str, Path]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    ano = current_time[:4]
    mes = str(int(current_time[5:7]))
    data = str(current_time[:10])
    partitions = os.path.join(
        f"ano_particao={ano}", f"mes_particao={mes}", f"data_particao={data}"
    )

    base_path = os.path.join(os.getcwd(), "data", "comando", dfr.name)

    partition_path = os.path.join(base_path, partitions)

    if not os.path.exists(partition_path):
        os.makedirs(partition_path)

    df_path = os.path.join(partition_path, f"dados_{current_time}.csv")

    dfr.to_csv(df_path, index=False)
    log(f">>>>>>> base_path {base_path}")
    return base_path
