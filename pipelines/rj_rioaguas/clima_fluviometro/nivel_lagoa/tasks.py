# -*- coding: utf-8 -*-

"""
Tasks para pipeline de dados de nível da Lagoa Rodrigo de Freitas.
Fonte: Squitter.
"""
# pylint: disable= C0327,W0106,R0913,R0914
from datetime import timedelta
from pathlib import Path
from typing import Union, Tuple
import pandas as pd
import pendulum
from bs4 import BeautifulSoup
from prefect import task

from pipelines.constants import constants

# from pipelines.rj_cor.meteorologia.utils import save_updated_rows_on_redis
from pipelines.rj_rioaguas.utils import login
from pipelines.utils.utils import (
    get_vault_secret,
    log,
    to_partitions,
    parse_date_columns,
    get_redis_client,
)


def save_updated_rows_on_redis(
    dfr: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    unique_id: str = "id_estacao",
    date_column: str = "data_medicao",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    mode: str = "prod",
) -> pd.DataFrame:
    """
    Acess redis to get the last time each unique_id was updated, return
    updated unique_id as a DataFrame and save new dates on redis
    """

    redis_client = get_redis_client()

    key = dataset_id + "." + table_id
    if mode == "dev":
        key = f"{mode}.{key}"

    # Access all data saved on redis with this key
    updates = redis_client.hgetall(key)

    # Convert data in dictionary in format with unique_id in key and last updated time as value
    # Example > {"12": "2022-06-06 14:45:00"}
    updates = {k.decode("utf-8"): v.decode("utf-8") for k, v in updates.items()}

    # Convert dictionary to dfr
    updates = pd.DataFrame(updates.items(), columns=[unique_id, "last_update"])
    log(f">>> data saved in redis: {updates}")
    # dfr and updates need to have the same index, in our case unique_id
    missing_in_dfr = [
        i for i in updates[unique_id].unique() if i not in dfr[unique_id].unique()
    ]
    log(f">>> data missing_in_dfr: {missing_in_dfr}")
    missing_in_updates = [
        i for i in dfr[unique_id].unique() if i not in updates[unique_id].unique()
    ]
    log(f">>> data missing_in_updates: {missing_in_updates}")
    log(f">>> old dfr: {dfr.iloc[3]}")
    log(f">>> old dfr: {dfr.iloc[1]}")
    log(f">>> old dfr: {dfr.iloc[2]}")
    # If unique_id doesn't exists on updates we create a fake date for this station on updates
    if len(missing_in_updates) > 0:
        for i in missing_in_updates:
            updates = updates.append(
                {unique_id: i, "last_update": "1900-01-01 00:00:00"},
                ignore_index=True,
            )

    # If unique_id doesn't exists on dfr we remove this stations from updates
    if len(missing_in_dfr) > 0:
        updates = updates[~updates[unique_id].isin(missing_in_dfr)]

    # Set the index with the unique_id
    # dfr.set_index(dfr[unique_id].unique(), inplace=True)
    # updates.set_index(updates[unique_id].unique(), inplace=True)
    log(f">>> new dfr: {dfr}")
    log(f">>> new dfr: {dfr.iloc[0]}")
    log(f">>> new updates: {updates}")
    log(f">>> new updates: {updates.iloc[0]}")
    # Merge dfs using unique_id
    dfr = dfr.merge(updates, how="left", on=unique_id)
    log(f">>>df merge: {dfr}")
    # Keep on dfr only the stations that has a time after the one that is saved on redis
    date_cols = [date_column, "last_update"]
    dfr[date_cols] = dfr[date_cols].apply(pd.to_datetime, format=date_format)
    a = dfr[dfr[date_column] > dfr["last_update"]].copy()
    log(f">>> data to save in redis as a dataframe: {a}")
    dfr = dfr[dfr[date_column] > dfr["last_update"]].dropna(subset=[unique_id])
    log(f">>> data to save in redis as a dataframe2: {dfr}")
    # Keep only the last date for each unique_id
    keep_cols = [unique_id, date_column]
    new_updates = dfr[keep_cols].sort_values(keep_cols).copy()
    new_updates.drop_duplicates(subset=unique_id, keep="last")
    log(f">>> new_updates: {new_updates}")
    # Convert stations with the new updates dates in a dictionary
    new_updates.set_index(unique_id, inplace=True)
    new_updates = dfr["last_update"].astype(str).to_dict()
    log(f">>> data to save in redis as a dict: {new_updates}")
    # Save this new information on redis
    [redis_client.hset(key, k, v) for k, v in new_updates.items()]

    return dfr.reset_index()


@task
def download_file(download_url):
    """
    Função para download de tabela com os dados.

    Args:
    download_url (str): URL onde a tabela está localizada.
    """
    # Acessar username e password
    dicionario = get_vault_secret("rioaguas_nivel_lagoa_squitter")
    url = dicionario["data"]["url"]
    user = dicionario["data"]["user"]
    password = dicionario["data"]["password"]
    session = login(url, user, password)
    page = session.get(download_url)
    soup = BeautifulSoup(page.text, "html.parser")
    table = soup.find_all("table")
    dfr = pd.read_html(str(table))[0]
    return dfr


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def tratar_dados(
    dfr: pd.DataFrame, dataset_id: str, table_id: str, mode: str = "prod"
) -> Tuple[pd.DataFrame, bool]:
    """
    Tratar dados para o padrão estabelecido.
    """

    # Renomeia colunas
    dfr = dfr.rename(
        columns={"Hora Leitura": "data_medicao", "Nível [m]": "lamina_nivel"}
    )
    # Adiciona coluna para id e nome da lagoa
    dfr["id_estacao"] = "1"
    dfr["nome_estacao"] = "Lagoa rodrigo de freitas"
    # Remove duplicados
    dfr = dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")
    # Cria id único para ser salvo no redis e comparado com demais dados salvos
    dfr["id"] = dfr["id_estacao"] + "_" + dfr["data_medicao"]
    # Acessa o redis e mantem apenas linhas que ainda não foram salvas
    log(f"[DEBUG]: dados coletados\n{dfr.tail()}")
    dfr = save_updated_rows_on_redis(
        dfr, dataset_id, table_id, unique_id="id_estacao", mode=mode
    )
    log(f"[DEBUG]: dados que serão salvos\n{dfr.tail()}")

    # If df is empty stop flow
    empty_data = dfr.shape[0] == 0
    log(f"[DEBUG]: dataframe is empty: {empty_data}")

    return (
        dfr[["data_medicao", "id_estacao", "nome_estacao", "lamina_nivel"]],
        empty_data,
    )


@task
def salvar_dados(dados: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados em csv.
    """
    prepath = Path("/tmp/nivel_lagoa/")
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
