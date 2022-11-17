# -*- coding: utf-8 -*-
"""
Tasks for the example flow
"""

from io import StringIO
import os

from pathlib import Path
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
    dfr = pd.read_csv(StringIO(data))
    log("Dados convertidos em DataFrame com sucesso!")
    return dfr


@task
def save_report(dataframe: pd.DataFrame) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        dataframe (pd.DataFrame): DataFrame do Pandas.
    """
    save_path = Path("temp", "report.csv")
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    dataframe.to_csv(save_path, index=False)
    log("Dados salvos em report.csv com sucesso!")
    return save_path
