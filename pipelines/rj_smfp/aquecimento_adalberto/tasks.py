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
    df = pd.read_csv(StringIO(data))
    log("Dados convertidos em DataFrame com sucesso!")
    return df

@task
def format_phone_field(df, colname):

    if colname in df.columns:
        
        def formatPhone(x):
            x = ''.join(c for c in x if c.isdigit())   # Removendo os caracters não numéricos
            x = x.zfill(8)   # Completando com zeros a esquerda caso tenha menos de 7 numeros
            x = x[-8:]   # pegando os 7 números da direita para a esquerda
            x = x[:4] + '-' + x[4:]
            return x   # retorno da função

        df[colname] = df[colname].apply(formatPhone) # Aplicando a função na coluna do dataframe
        
    else:
        log("Variavel nao existe!")
    return df	

@task
def save_report(dataframe: pd.DataFrame) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        dataframe (pd.DataFrame): DataFrame do Pandas.
    """
    dataframe.to_csv("report.csv", index=False)
    log("Dados salvos em report.csv com sucesso!")