# -*- coding: utf-8 -*-
"""
General purpose functions for the comando project
"""
import requests
from pipelines.utils.utils import get_vault_secret


def get_token():
    """Get token to access comando's API"""
    # Acessar username e password
    dicionario = get_vault_secret("comando")
    host = dicionario["data"]["host"]
    username = dicionario["data"]["username"]
    password = dicionario["data"]["password"]
    payload = {"username": username, "password": password}
    return requests.post(host, json=payload).text


def get_url(url, parameters={}):  # pylint: disable=W0102
    """Make request to comando's API"""
    token = get_token()
    headers = {"Authorization": token}
    return requests.get(url, json=parameters, headers=headers).json()
