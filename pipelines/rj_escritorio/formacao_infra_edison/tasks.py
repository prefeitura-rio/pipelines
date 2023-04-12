# -*- coding: utf-8 -*-
from prefect import task
import requests
import pandas as pd
from pandas import json_normalize
import json
from pipelines.utils.utils import exibeLog
from pathlib import Path
from uuid import uuid4


@task
def coletaDado(quantidadeDado: int):
    """Função para coletar uma quantidade n de dados de API geradora randomica de usuarios

    Args:
        quantidadeDado (int): quantidade de usuarios a serem coletados

    Returns:
        _response_: estrutura com os usuarios
    """
    parametroAPI = "https://randomuser.me/api/?results=" + str(quantidadeDado)
    requests.get(parametroAPI)
    exibeLog("Dados baixados com sucesso!")


@task
def trataDado(dados):
    """Função para transformar dados coletados normalizando o Dataframe

    Args:
        response (requests): dados coletados da API

    Returns:
        _response_: Dataframe
    """
    transformaDados = dados.json(["results"])
    json_normalize(transformaDados)
    exibeLog("Dados convertidos em DataFrame com sucesso!")


@task
def dataframe_to_csv(dataframe: pd.DataFrame, filename: str = "data.csv") -> None:
    """
    Save dataframe to csv
    """
    filename = filename if filename.endswith(".csv") else f"{filename}.csv"
    temp_filename = Path(f"/tmp/{uuid4()}/{filename}")
    temp_filename.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(temp_filename, index=False)
    exibeLog("Dados salvos em data.csv com sucesso!")
