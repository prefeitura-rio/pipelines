# -*- coding: utf-8 -*-
"""
General purpose functions for the comando project
"""
# pylint: disable=W0611
from urllib.error import HTTPError
import requests
from requests.adapters import HTTPAdapter, Retry

import pandas as pd
import pendulum
from pipelines.utils.utils import (
    get_vault_secret,
    log,
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


def download_data(first_date, last_date, url) -> pd.DataFrame:
    """
    Download data from API adding one day at a time so we can save
    the date in a new column `data_particao` that will be used to
    create the partitions when saving data.
    """
    dfr = pd.DataFrame()
    temp_date = first_date.add(days=1)
    fmt = "%d/%m/%Y"
    while temp_date <= last_date:
        log(f"\n\nDownloading data from {first_date} to {temp_date} (not included)")
        try:
            dfr_temp = pd.read_json(
                f"{url}/?data_i={first_date.strftime(fmt)}&data_f={temp_date.strftime(fmt)}"
            )
            dfr_temp["create_partition"] = first_date.strftime("%Y-%m-%d")
            dfr = pd.concat([dfr, dfr_temp])
        except HTTPError as error:
            print(f"Error downloading this data: {error}")
        first_date, temp_date = first_date.add(days=1), temp_date.add(days=1)
    return dfr
