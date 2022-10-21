# -*- coding: utf-8 -*-
'''tasks'''
from io import StringIO
import requests

import pandas as pd
from prefect import task
from pipelines.formacao.utils import log
from pipelines.formacao.utils import df_formatnum

@task
def download_data (n_users : int) -> str:
    '''Funcao que recebe o numero de usuarios randomicos a serem gerados 
    e retorna uma string
    
    Args:
        n: Número de usuários aleatórios.

    Returns:
        str: Texto em formato CSV
    '''
    
    response = requests.get("https://randomuser.me/api/?results="+str(n_users)+"&format=csv")
   
    return response.text

@task
def to_dataframe(data) -> pd.DataFrame:
    """
    Recebe o texto com os dados, formata os números e converte para um
    dataframe do pandas.

    Args:
        data (str): Dados em CSV formato Byte.
    
    Returns:    
        pd.DataFrame: DataFrame do Pandas.
    """
    dframe = pd.read_csv(StringIO(data))
    return df_formatnum(dframe)

@task
def save_report(dataframe: pd.DataFrame) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        dataframe (pd.DataFrame): DataFrame do Pandas.
    
    Returns:    
        None
    """
    dataframe.to_csv("report.csv", index=False)
    log("Dados salvos em report.csv com sucesso!")
  