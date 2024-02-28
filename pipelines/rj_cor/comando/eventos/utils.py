# -*- coding: utf-8 -*-
"""
General purpose functions for the comando project
"""
# pylint: disable=W0611
import json
import requests
from requests.adapters import HTTPAdapter, Retry

import pendulum
import pandas as pd
from pipelines.utils.utils import (
    get_redis_client,
    get_vault_secret,
    log,
    treat_redis_output,
)


def format_date(first_date, last_date):
    """
    Format date to "dd/mm/yyyy" and add one day to last date because
    the API has open interval at the end: [first_date, last_date).
    """
    first_date = pendulum.from_format(first_date, "YYYY-MM-DD").strftime("%d/%m/%Y")
    last_date = (
        pendulum.from_format(last_date, "YYYY-MM-DD").add(days=1).strftime("%d/%m/%Y")
    )
    return first_date, last_date


def get_redis_output(redis_key, is_df: bool = False):
    """
    Get Redis output. Use get to obtain a df from redis or hgetall if is a key value pair.
    """
    redis_client = get_redis_client()  # (host="127.0.0.1")

    if is_df:
        json_data = redis_client.get(redis_key)
        log(f"[DEGUB] json_data {json_data}")
        if json_data:
            # If data is found, parse the JSON string back to a Python object (dictionary)
            data_dict = json.loads(json_data)
            # Convert the dictionary back to a DataFrame
            return pd.DataFrame(data_dict)

        return pd.DataFrame()

    output = redis_client.hgetall(redis_key)
    if len(output) > 0:
        output = treat_redis_output(output)
    return output


def compare_actual_df_with_redis_df(
    dfr: pd.DataFrame,
    dfr_redis: pd.DataFrame,
    columns: list,
) -> pd.DataFrame:
    """
    Compare df from redis to actual df and return only the rows from actual df
    that are not already saved on redis.
    """
    for col in columns:
        if col not in dfr_redis.columns:
            dfr_redis[col] = None
        dfr_redis[col] = dfr_redis[col].astype(dfr[col].dtypes)
    log(f"\nEnded conversion types from dfr to dfr_redis: \n{dfr_redis.dtypes}")

    dfr_diff = (
        pd.merge(dfr, dfr_redis, how="left", on=columns, indicator=True)
        .query('_merge == "left_only"')
        .drop("_merge", axis=1)
    )
    log(
        f"\nDf resulted from the difference between dft_redis and dfr: \n{dfr_diff.head()}"
    )

    updated_dfr_redis = pd.concat([dfr_redis, dfr_diff[columns]])

    return dfr_diff, updated_dfr_redis


def treat_wrong_id_pop(dfr):
    """
    Create id_pop based on pop_titulo column
    """
    pop = {
        "Ajuste de banco": 0,
        "Acidente/enguiço sem vítima": 1,
        "Acidente com vítima(s)": 2,
        "Acidente com vítima(s) fatal(is)": 3,
        "Incêndio em veículo(s)": 4,
        "Bolsão d'água em via": 5,
        "Alagamentos e enchentes": 6,
        "Manifestação em local público": 7,
        "Incêndio em imóvel": 8,
        "Sinais de trânsito com mau funcionamento": 9,
        "Reintegração de posse": 10,
        "Queda de árvore": 11,
        "Queda de poste": 12,
        "Acidente com queda de carga": 13,
        "Incêndio no entorno de vias públicas": 14,
        "Incêndio dentro de túneis": 15,
        "Vazamento de água / esgoto": 16,
        "Falta de luz / apagão": 17,
        "Implosão": 18,
        "Queda de estrutura de alvenaria": 19,
        "Vazamento de gás": 20,
        "Evento em local público ou particular": 21,
        "Atropelamento": 22,
        "Afundamento de pista / buraco na via": 23,
        "Abalroamento": 24,
        "Obra em local público": 25,
        "Operação policial": 26,
        "Bloco de rua": 27,
        "Deslizamento": 28,
        "Animal em local público": 29,
        "Acionamento de sirenes": 30,
        "Alagamento": 31,
        "Enchente": 32,
        "Lâmina d'água": 33,
        "Acidente ambiental": 34,
        "Bueiro": 35,
        "Incidente com bueiro": 35,
        "Resgate ou remoção de animais terrestres e aéreos": 36,
        "Remoção de animais mortos na areia": 37,
        "Resgate de animal marinho preso em rede / encalhado": 38,
        "Incendio em vegetacao": 39,
        "Queda de árvore sobre fiação": 40,
        "Residuos na via": 41,
        "Evento não programado": 99,
    }
    dfr["id_pop"] = dfr["pop_titulo"].map(pop)
    return dfr


def build_redis_key(dataset_id: str, table_id: str, name: str, mode: str = "prod"):
    """
    Helper function for building a key where to store the last updated time
    """
    key = dataset_id + "." + table_id + "." + name
    if mode == "dev":
        key = f"{mode}.{key}"
    return key


def get_token():
    """Get token to access comando's API"""
    # Acessar username e password
    dicionario = get_vault_secret("comando")
    host = dicionario["data"]["host"]
    username = dicionario["data"]["username"]
    password = dicionario["data"]["password"]
    payload = {"username": username, "password": password}
    return requests.post(host, json=payload).text


# pylint: disable=W0703
def get_url(url, parameters: dict = None, token: str = None):  # pylint: disable=W0102
    """Make request to comando's API"""
    if not parameters:
        parameters = {}
    if not token:
        token = get_token()
    sess = requests.Session()
    retries = Retry(total=5, backoff_factor=1.5)
    sess.mount("http://", HTTPAdapter(max_retries=retries))
    headers = {"Authorization": token}

    try:
        response = sess.get(url, json=parameters, headers=headers)
        response = response.json()
    except Exception as exc:
        log(f"This resulted in the following error: {exc}")
        response = {"response": None}
    return response
