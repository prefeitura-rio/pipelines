# -*- coding: utf-8 -*-
"""
Tasks for precipitacao_alertario
"""
from datetime import timedelta
import ftplib
import os
from pathlib import Path
import socket
from typing import Union, Tuple

import numpy as np
import pandas as pd
import pendulum
from prefect import task

from pipelines.constants import constants
from pipelines.utils.utils import get_vault_secret, log

###################################################################################
#        Ver de trocar o nome da coluna data_medição e deixar padronizado         #
###################################################################################


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download() -> Tuple[pd.DataFrame, str]:
    """
    Faz o request e salva dados localmente
    """

    # Acessar FTP Riomidia
    dirname = "/alertario/"
    filename = "EstacoesMapa.txt"

    # Acessar username e password
    dicionario = get_vault_secret("riomidia")
    host = dicionario["data"]["host"]
    username = dicionario["data"]["username"]
    password = dicionario["data"]["password"]

    # Cria pasta para salvar arquivo de download
    base_path = os.path.join(os.getcwd(), "data", "precipitacao_alertario", "input")

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    save_on = os.path.join(base_path, filename)

    try:
        ftp = ftplib.FTP(host)
    except (socket.error, socket.gaierror):
        log(f"ERROR: cannot reach {host}")
        raise
    log(f"*** Connected to host {host}")

    try:
        ftp.login(username, password)
    except ftplib.error_perm:
        log("ERROR: cannot login")
        ftp.quit()
        raise
    log("*** Logged in successfully")

    try:
        ftp.cwd(dirname)
    except ftplib.error_perm:
        log(f"ERROR: cannot CD to {dirname}")
        ftp.quit()
        raise
    log(f"*** Changed to folder: {dirname}")

    try:
        with open(save_on, "wb") as file:
            log("Getting " + filename)
            ftp.retrbinary("RETR " + filename, file.write)
        log(f"File downloaded to {save_on}")
    except ftplib.error_perm:
        log(f"ERROR: cannot read file {filename}")
        raise

    ftp.quit()

    # Hora atual no formato YYYYMMDDHHmm para criar partições
    current_time = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")

    return save_on, current_time


@task
def tratar_dados(filename: Union[str, Path]) -> pd.DataFrame:
    """
    Renomeia colunas e filtra dados com a hora e minuto do timestamp
    de execução mais próximo à este
    """

    colunas = [
        "id",
        "estacao",
        "localizacao",
        "data_medicao",
        "latitude",
        "longitude",
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
        "acumulado_chuva_mes",
    ]

    dados = pd.read_csv(filename, skiprows=1, sep=";", names=colunas, decimal=",")

    # Adequando formato de data
    dados["data_medicao"] = pd.to_datetime(
        dados["data_medicao"], format="%H:%M - %d/%m/%Y"
    )

    # Ordenação de variáveis
    cols_order = [
        "data_medicao",
        "estacao",
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
    ]

    dados = dados[cols_order]

    # Mantendo primeira letra maiúscula na variável categória
    dados["estacao"] = dados["estacao"].str.capitalize()

    # Alterando valores ND, '-' e np.nan para NULL
    dados.replace(["ND", "-", np.nan], [None, None, None], inplace=True)

    # Converte variáveis que deveriam ser float para float
    float_cols = [
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
    ]
    dados[float_cols] = dados[float_cols].apply(pd.to_numeric, errors="coerce")

    # Normaliza nomes das estações e troca por id
    dados.estacao = dados.estacao.str.normalize('NFKD').str.encode('ascii',errors='ignore').str.decode('utf-8')
    dados.estacao = dados.estacao.replace({
        'Jacarepagua/tanque': 'Tanque',
        'Jacarepagua/cidade de deus': 'Cidade de deus',
        'Barra/barrinha': 'Barrinha',
        'Barra/riocentro': 'Riocentro',
        'Est. grajau/jacarepagua': 'Grajau jacarepagua',
        'Av. brasil/mendanha': 'Av brasil mendanha',
        'Recreio dos bandeirantes': 'Recreio',
        'Tijuca/muda': 'Tijuca muda'}
    )

    map_estacoes = {"Ilha do governador": 8, "Jardim botanico": 16, "Riocentro": 19,
        "Guaratiba": 20, "Barrinha": 17, "Recreio": 30, "Grota funda": 25, "Bangu": 12,
        "Saude": 15, "Cidade de deus": 18, "Santa cruz": 22, "Iraja": 11, "Grande meier": 23,
        "Sao cristovao": 32, "Campo grande": 26, "Av brasil mendanha": 29, "Tijuca muda": 33,
        "Madureira": 10, "Piedade": 13, "Anchieta": 24, "Laranjeiras": 31, "Sepetiba": 27,
        "Tanque": 14, "Grajau": 7, "Tijuca": 4, "Vidigal": 1, "Urca": 2, "Copacabana": 6,
        "Alto da boa vista": 28, "Grajau jacarepagua": 21, "Penha": 9, "Rocinha": 3,
        "Santa teresa": 5}
    dados['estacao'] = dados.estacao.replace(map_estacoes)

    dados = dados.rename(columns={'estacao': 'id_estacao'})

    dados['id_estacao'] = dados['id_estacao'].astype(int)

    return dados


@task
def salvar_dados(
    dados: pd.DataFrame, current_time: str
) -> Union[str, Path]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    ano = current_time[:4]
    mes = str(int(current_time[4:6]))
    dia = str(int(current_time[6:8]))
    partitions = os.path.join(f"ano={ano}", f"mes={mes}", f"dia={dia}")

    base_path = os.path.join(
        os.getcwd(), "data", "precipitacao_alertario", "output"
    )

    partition_path = os.path.join(base_path, partitions)

    if not os.path.exists(partition_path):
        os.makedirs(partition_path)

    filename = os.path.join(partition_path, f"dados_{current_time}.csv")

    log(f"Saving {filename}")
    dados.to_csv(filename, index=False)
    return base_path
