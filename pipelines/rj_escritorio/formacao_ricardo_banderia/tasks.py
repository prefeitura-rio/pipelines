# -*- coding: utf-8 -*-
import requests
import json
import numpy as np
import pandas as pd
from pandas import json_normalize
from pandas_profiling import ProfileReport
from pipelines.utils.utils import log

from prefect import task

# from pipelines.utils.utils import log


# define a url base da api
URL_API = "https://randomuser.me/api/"

# define caminho do arquivo a ser gravado
FILE_NAME = "dados/out.csv"

# Etapa 1: Entendendo os dados
# ===================================


@task
def get_dic_dados(url):
    response = requests.get(url)
    return response


# ===================================

# Etapa 2: Coletando dados
# ===================================

# função para retornar dataframe a partir de requisição JSON (recebendo parâmetros)


@task
def get_dataframe(url, qtd, parameters):
    # cria um dataframe a partir de uma requisição à api

    # define a quantidade de resultados desejada
    url_qtd_results = f"{url}?results={qtd}"

    # faz a requisição passando os parâmetros
    response = requests.get(url_qtd_results, params=parameters)

    # define o formato para JSON
    resp_json = response.json()

    # converte para dataframe
    df = pd.DataFrame(resp_json["results"])

    return df


# função para retornar um dataframe a partir de requisiçãqo JSON
@task
def get_df(url, qtd):
    # cria um dataframe a partir de uma requisição à api

    # define a quantidade de resultados desejada
    url_qtd_results = f"{url}?results={qtd}"

    # faz a requisição passando os parâmetros
    response = requests.get(url_qtd_results)

    # define o formato para JSON
    resp_json = response.json()

    # converte para dataframe
    df = pd.DataFrame(resp_json["results"])

    return df


@task
def save_dataframe(df, path):
    from pathlib import Path

    filepath = Path(path)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath)


# ===================================

# 4: Analisando dados sem agrupamento
# ===================================

# função que recebe um dataframe e gera um relatório em HTML
@task
def report_from_df(df):
    profile = ProfileReport(df, title="Relátorio Atividade 4")
    profile.to_file("Report.html")


# ===================================

# Etapa 5: Analisando dados com agrupamento
# ===================================

# faz a requisição passando os parâmetros
# response = requests.get(url_api + "?results=10&inc=location")

# função para retornar um dataframe agrupado


@task
def df_groupby(url, query, group):

    # monta a url com a query
    url_query = f"{url}?{query}"

    # faz a requisição
    response = requests.get(url_query)

    # define o formato para JSON
    resp_json = response.json()

    # converte para dataframe
    df = pd.DataFrame(resp_json["results"])

    # normaliza o JSON
    # dfn = pd.json_normalize(resp_json["results"])

    # agrupa o dataframe
    dfg = df.groupby([group])

    return dfg


# função para agrupar dataframe por país e estado
@task
def df_group(df, group):
    dfg = df.groupby([group])
    return dfg


# imprime o dataframe agrupado
# @task
# def print_df(df):
#     for key, item in dfg:
#         print(dfg.get_group(key), "\n\n")
