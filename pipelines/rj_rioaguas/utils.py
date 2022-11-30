# -*- coding: utf-8 -*-
"""
Utils Gerais da Rio-Águas.
"""

import requests


def login(url, user, password):
    """
    Função para fazer login no website.

    Args:
    url (str): URL da página inicial com os campos de usuário e senha.
    user (str): Usuário do login
    password (str): Senha de acesso.
    """

    client = requests.Session()

    # Retrieve the CSRF token first
    client.get(url)  # sets cookie
    if "csrftoken" in client.cookies:
        # Django 1.6 and up
        csrftoken = client.cookies["csrftoken"]
    else:
        # older versions
        csrftoken = client.cookies["csrf"]

    payload = {
        "username": user,
        "password": password,
        "csrfmiddlewaretoken": csrftoken,
        "next": "/",
    }

    client.post(url, data=payload, headers=dict(Referer=url))

    return client
