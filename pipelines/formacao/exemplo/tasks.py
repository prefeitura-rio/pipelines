# -*- coding: utf-8 -*-
"""
    Tasks for example flow
"""
from io import StringIO
import re

import requests
import pandas as pd

from prefect import task

from pipelines.utils.utils import log


@task
def download_data(n_users: int) -> str:
    """
    Baixa dados da API https://randomuser.me e retorna um texto em formato CSV.

    Args:
        n_users (int): número de usuários a serem baixados.

    Returns:
        str: texto em formato CSV.
    """
    response = requests.get(
        "https://randomuser.me/api/?results={}&format=csv".format(n_users)
    )
    log("Dados baixados com sucesso!")
    return response.text


@task
def gerar_df(data: str) -> pd.DataFrame:
    """
    Transforma os dados em formato CSV em um DataFrame do Pandas, para facilitar sua manipulação.

    Args:
        data (str): texto em formato CSV.

    Returns:
        pd.DataFrame: DataFrame do Pandas.
    """
    df = pd.read_csv(StringIO(data))
    log("Dados convertidos em DataFrame com sucesso!")
    return df


@task
def format_phone_number(df, column_name):
    """
    Formata o número de telefone

     Args:
        dataframe (pd.DataFrame): DataFrame do Pandas, column
    """

    def get_number(number):
        """remover caracteres não numericos"""
        return re.sub(r"[^0-9]", "", number)

    df[column_name] = df[column_name].apply(get_number)

    def format_number(number):
        """formata o numero de telefone"""
        return f"{number[:2]} {number[2:]}"

    df[column_name] = df[column_name].apply(format_number)
    log("Dados formatados com sucesso!")
    df.to_csv("/tmp/report.csv", index=False)
    return "/tmp/report.csv"
