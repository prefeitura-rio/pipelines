# -*- coding: utf-8 -*-
"""
General purpose functions for the comando project
"""
import json
import requests
from requests.adapters import HTTPAdapter, Retry

from pipelines.utils.utils import get_vault_secret, log


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


# pylint: disable=W0703,W0611
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

    log(f"\n\n>>>>>>> headers {headers} \n\n")
    log(f"\n\n>>>>>>> parameters {parameters} \n\n")
    try:
        response = sess.get(url, json=parameters, headers=headers)
        response = response.json()
    except Exception as exc:
        log(f"This resulted in the following error: {exc}")
        response = {"response": None}
    return response
