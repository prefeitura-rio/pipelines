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
from pipelines.rj_cor.meteorologia.utils import save_updated_rows_on_redis
from pipelines.utils.utils import get_vault_secret, log


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

    # Hora atual no formato YYYYMMDDHHmm para criar parti????es
    current_time = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")

    return save_on, current_time


@task(nout=2)
def tratar_dados(
    filename: Union[str, Path], dataset_id: str, table_id: str
) -> Tuple[pd.DataFrame, bool]:
    """
    Renomeia colunas e filtra dados com a hora e minuto do timestamp
    de execu????o mais pr??ximo ?? este
    """

    colunas = [
        "id_estacao",
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

    # Ordena????o de vari??veis
    cols_order = [
        "data_medicao",
        "id_estacao",
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
    ]

    dados = dados[cols_order]

    # Alterando valores ND, '-' e np.nan para NULL
    dados.replace(["ND", "-", np.nan], [None, None, None], inplace=True)

    # Converte vari??veis que deveriam ser float para float
    float_cols = [
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
    ]
    dados[float_cols] = dados[float_cols].apply(pd.to_numeric, errors="coerce")

    # Altera valores negativos para 0
    dados[float_cols] = np.where(dados[float_cols] < 0, None, dados[float_cols])

    # Elimina linhas em que o id_estacao ?? igual mantendo a de menor valor nas colunas float
    dados.sort_values(["id_estacao", "data_medicao"] + float_cols, inplace=True)
    dados.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    log(f"uniquesss df >>>, {type(dados.id_estacao.unique()[0])}")
    dados["id_estacao"] = dados["id_estacao"].astype(str)

    dados = save_updated_rows_on_redis(dados, dataset_id, table_id, mode="dev")

    dados["id_estacao"] = dados["id_estacao"].astype(int)

    # Fixar ordem das colunas
    dados = dados[
        [
            "data_medicao",
            "id_estacao",
            "acumulado_chuva_15_min",
            "acumulado_chuva_1_h",
            "acumulado_chuva_4_h",
            "acumulado_chuva_24_h",
            "acumulado_chuva_96_h",
        ]
    ]

    # If df is empty stop flow
    empty_data = dados.shape[0] == 0

    return dados, empty_data


@task
def salvar_dados(dados: pd.DataFrame, current_time: str) -> Union[str, Path]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    ano = current_time[:4]
    mes = str(int(current_time[4:6]))
    dia = str(int(current_time[6:8]))
    partitions = os.path.join(f"ano={ano}", f"mes={mes}", f"dia={dia}")

    base_path = os.path.join(os.getcwd(), "data", "precipitacao_alertario", "output")

    partition_path = os.path.join(base_path, partitions)

    if not os.path.exists(partition_path):
        os.makedirs(partition_path)

    filename = os.path.join(partition_path, f"dados_{current_time}.csv")

    log(f"Saving {filename}")
    dados.to_csv(filename, index=False)
    return base_path
