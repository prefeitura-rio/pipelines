# -*- coding: utf-8 -*-
# pylint: disable= line-too-long

"""
Tasks for nivel_reservatorio project
"""

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
    Baixa dados da planilha google e retorna um texto em formato CSV..

    Returns:
        str: texto em formato CSV.
    """
    response = requests.get(
        "https://docs.google.com/spreadsheets/d/1zM0N_PonkALEK3YD2A4DF9W10Cm2n99_IiySm8zygqk/edit#gid=1343658906",  # noqa
        timeout=5,
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
