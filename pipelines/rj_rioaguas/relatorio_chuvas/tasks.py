# -*- coding: utf-8 -*-

"""
Tasks para pipeline de dados de nível da Lagoa Rodrigo de Freitas.
Fonte: Squitter.
"""
from pathlib import Path
from typing import Union
import os
import pandas as pd

from bs4 import BeautifulSoup
from prefect import task
from pipelines.utils.utils import get_vault_secret, log
from pipelines.rj_rioaguas.utils import login


@task
def download_file(download_url):
    '''
    Função para download de tabela com os dados.

    Args:
    download_url (str): URL onde a tabela está localizada.
    '''
    # Acessar username e password
    dicionario = get_vault_secret("rioaguas_nivel_lagoa_squitter")
    url = dicionario["data"]["url"]
    user = dicionario["data"]["user"]
    password = dicionario["data"]["password"]
    session = login(url, user, password)
    # download_url = "http://horus.squitter.com.br/dados/meteorologicos/292/"
    page = session.get(download_url)
    soup = BeautifulSoup(page.text, 'html.parser')
    table = soup.find_all('table')
    dfr = pd.read_html(str(table))[0]
    return dfr


@task
def salvar_dados(dados: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados em csv.
    """
    base_path = Path(os.getcwd())
    # partition_path = Path(base_path, partitions)
    # if not os.path.exists(partition_path):
    #     os.makedirs(partition_path)
    filename = str(base_path / "nivel.csv")
    log(f"Saving {filename}")
    # dados.to_csv(filename, index=False)
    dados.to_csv(r"{}".format(filename), index=False)
    return base_path
