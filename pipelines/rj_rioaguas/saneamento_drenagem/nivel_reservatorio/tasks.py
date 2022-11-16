# -*- coding: utf-8 -*-

from io import StringIO
from pathlib import Path
import os

import pandas as pd
from prefect import task
import requests

from pipelines.utils.utils import log


@task
def download_data() -> str:
    """
    Baixa dados da planilha google
    https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vQd3-V6K_hOcrVySYJKk0tevS9TCI0MpwQ5W7IY-_fIUUR4uZ0JVttqmaHeA9Pm-BJsAXUmjTvLZaDt/pubhtml?widget=true&headers=false#gid=1343658906
    e retorna um texto em formato CSV.

    Args:
        n_users (int): número de usuários a serem baixados.

    Returns:
        str: texto em formato CSV.
    """
    response = requests.get(
        "https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vQd3-V6K_hOcrVySYJKk0tevS9TCI0MpwQ5W7IY-_fIUUR4uZ0JVttqmaHeA9Pm-BJsAXUmjTvLZaDt/pubhtml?widget=true&headers=false#gid=1343658906"
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
    df = phone_n_formatter(pd.read_csv(StringIO(data)))
    log("Dados convertidos em DataFrame com sucesso!")
    return df


@task
def save_report(dataframe: pd.DataFrame) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        dataframe (pd.DataFrame): DataFrame do Pandas.
    """

    base_path = Path(os.getcwd(), "data", "output")

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    dataframe.to_csv(
        f"{base_path}/nivel_reservatorio.csv",
        sep=";",
        index=False,
    )
    log("Dados salvos em report.csv com sucesso!")
