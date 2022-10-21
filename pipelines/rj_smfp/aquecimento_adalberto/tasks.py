# -*- coding: utf-8 -*-
"""
Tasks do Adalberto
"""
from io import StringIO

import pandas as pd
from prefect import task
import requests

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
def parse_data(data: str) -> pd.DataFrame:
    """
    Transforma os dados em formato CSV em um DataFrame do Pandas, para facilitar sua manipulação.

    Args:
        data (str): texto em formato CSV.

    Returns:
        pd.DataFrame: DataFrame do Pandas.
    """
    dataframe = pd.read_csv(StringIO(data))
    log("Dados convertidos em DataFrame com sucesso!")
    return dataframe


@task
def format_phone_field(dataframe, colname):
    """
    Format phones
    """

    if colname in dataframe.columns:

        def format_phone(phoneno):
            # Removendo os caracters não numéricos
            phoneno = "".join(c for c in phoneno if c.isdigit())
            # Completando com zeros a esquerda caso tenha menos de 7 numeros
            phoneno = phoneno.zfill(8)
            phoneno = phoneno[-8:]  # pegando os 7 números da direita para a esquerda
            phoneno = phoneno[:4] + "-" + phoneno[4:]
            return phoneno  # retorno da função

        # Aplicando a função na coluna do dataframe
        dataframe[colname] = dataframe[colname].apply(format_phone)

    else:
        log("Variavel nao existe!")
    return dataframe


@task
def save_report(dataframe: pd.DataFrame) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        dataframe (pd.DataFrame): DataFrame do Pandas.
    """
    dataframe.to_csv("report.csv", index=False)
    log("Dados salvos em report.csv com sucesso!")
