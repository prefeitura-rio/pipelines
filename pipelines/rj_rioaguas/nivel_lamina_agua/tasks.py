# -*- coding: utf-8 -*-

"""
Tasks para pipeline de dados de nível da Lagoa Rodrigo de Freitas.
Fonte: Squitter.
"""
# pylint: disable=C0327, E1120, W0108

from pathlib import Path
from typing import Union
import pandas as pd
import unidecode
from bs4 import BeautifulSoup

from prefect import task
from pipelines.utils.utils import get_vault_secret, log
from pipelines.rj_rioaguas.utils import login


@task
def download_file(download_url: str) -> pd.DataFrame:
    """
    Função para download de tabela com os dados.

    Args:
    download_url (str): URL onde a tabela está localizada.
    """
    # Acessar página web
    dicionario = get_vault_secret("rioaguas_lamina_agua")
    url = dicionario["data"]["url"]

    session = login(url)

    page = session.get(download_url)

    # Faz o parse do htm e seleciona apenas dados que estão em tabela
    soup = BeautifulSoup(page.text, "html.parser")
    table = soup.find_all("table")

    # Converte dados para dataframe
    dados = pd.read_html(str(table), flavor="bs4")[0]

    return dados


@task
def tratar_dados(dados: pd.DataFrame) -> pd.DataFrame:
    """Tratar dados para o padrão estabelecido e filtrar linhas para salvarmos apenas as medições
    que foram contratadas pela prefeitura. Atualmente, apenas o Catete e a Lagoa estão no contrato.
    """
    # Filtra apenas endereços contratados
    dados = dados[dados["Endereço"].isin(["Catete", "Lagoa"])].copy()

    rename_cols = {
        "Endereço": "endereco",
        "Último envio": "data_medicao",
        "Temperatura": "temperatura",
        "Umidade": "umidade",
        "Precipitação": "precipitacao",
        "Lâmina": "lamina_agua",
    }

    # Substitui valores que aparecem nas linhas
    dados = dados.rename(rename_cols, axis=1).replace(
        {
            " ºC": "",
            " %": "",
            " mm": "",
            " cm": "",
            ",": ".",
            "R:": "rua",
        },
        regex=True,
    )

    dados["endereco"] = dados["endereco"].str.capitalize()
    dados["endereco"] = dados["endereco"].apply(lambda x: unidecode.unidecode(x))
    dados["data_medicao"] = pd.to_datetime(
        dados["data_medicao"], format="%d/%m/%Y %H:%M"
    )

    return dados


@task
def salvar_dados(dados: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados em csv.
    """
    base_path = Path("/tmp/nivel_lamina_agua/")
    base_path.mkdir(parents=True, exist_ok=True)

    filename = base_path / "nivel.csv"
    log(f"Saving {filename}")
    dados.to_csv(filename, index=False)
    return base_path
