# -*- coding: utf-8 -*-
"""
Tasks for the example flow
"""
from io import StringIO

import os
import pandas as pd
from prefect import task
import requests
from matplotlib import pyplot as plt

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
    dataframe.to_csv("report.csv", index=False)
    log("Dados salvos em report.csv com sucesso!")


@task
def format_phone(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza os números de telefone para um formato único

    Args:
        dataframe (pd.DataFrame): DataFrame do Pandas.

    Returns:
        pd.DataFrame: DataFrame do Pandas.
    """
    dic = {"-": "", r"(": "", r")": ""}
    dataframe["phone"] = dataframe["phone"].replace(dic, regex=True)
    return dataframe


@task
def create_reports(dataframe: pd.DataFrame) -> None:
    """
    Gera arquivo texto com a percentagem dos usuários por gênero e por país
    e arquivo imagem com gráfico da distribuição por idade

    Args:
        dataframe (pd.DataFrame): DataFrame do Pandas.
    """

    # Cria o primeiro arquivo
    arquivo1 = open(os.path.join("Relatorio1.txt"), "w")
    arquivo1.write("Relatório Usuários por Gênero e Usuários por País")
    arquivo1.write("\n")
    arquivo1.write("\n")
    arquivo1.write("Percentual de Usuários por Gênero")
    arquivo1.write("\n")
    arquivo1.write(
        str(
            dataframe.groupby("gender")["login.uuid"].count()
            / dataframe["login.uuid"].count()
            * 100
        )
    )
    arquivo1.write("\n")
    arquivo1.write("\n")
    arquivo1.write("Percentual de Usuários por País")
    arquivo1.write("\n")
    arquivo1.write(
        str(
            dataframe.groupby("location.country")["login.uuid"].count()
            / dataframe["login.uuid"].count()
            * 100
        )
    )

    arquivo1.close()

    # Cria o segundo arquivo
    idade = dataframe["dob.age"]
    plt.hist(idade, bins=6, range=(idade.min(), idade.max()))
    plt.savefig("Distribuição por idade.jpg")


@task
def create_vision_country_state(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
     Gera um dataframe com total de pessoas agrupado por país e estado a partir do dataframe recebido

     Args:
         dataframe (pd.DataFrame): DataFrame do Pandas.
    Returns:
         pd.DataFrame: DataFrame do Pandas.
    """
    dataframe2 = dataframe.groupby(["location.country", "location.state"]).agg(
        qtdeusuarios=("login.uuid", "count")
    )
    return dataframe2
