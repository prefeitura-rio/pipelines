# -*- coding: utf-8 -*-
"""
General purpose functions for the comando project
"""
import requests
from requests.adapters import HTTPAdapter, Retry

from pipelines.utils.utils import get_vault_secret


def build_redis_key(dataset_id: str, table_id: str, mode: str = "prod"):
    key = dataset_id + "." + table_id
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


def get_url(url, parameters: dict = None, token: str = None):  # pylint: disable=W0102
    """Make request to comando's API"""
    if not parameters:
        parameters = {}
    if not token:
        token = get_token()
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1.5)
    s.mount("http://", HTTPAdapter(max_retries=retries))
    headers = {"Authorization": token}
    return s.get(url, json=parameters, headers=headers).json()
