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

###################################################################################
######## Ver de trocar o nome da coluna data_medição e deixar padronizado #########
###################################################################################

@task(nout=2,
      max_retries=constants.TASK_MAX_RETRIES.value,
      retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
    )
def download() -> Tuple[pd.DataFrame, str]:
    '''
    Faz o request na data especificado e retorna dados
    Tentando conseguir com o sardinha o ftp que permite
    termos os dados atualizados a cada 5 minutos
    '''
    # Acessar FTP Riomidia
    #### colocar no .env
    host = '187.111.97.195'
    dirname = '/alertario/'
    filename = 'EstacoesMapa.txt'
    username = 'escdados'
    password = ''

    base_path = os.path.join(os.getcwd(),'data', 'precipitacao_alertario', 'input')

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    save_on = os.path.join(base_path, filename)

    try:
        ftp = ftplib.FTP(host)
    except (socket.error, socket.gaierror):
        print(f'ERROR: cannot reach {host}')
        raise
    print(f'*** Connected to host {host}')

    try:
        ftp.login(username, password)
    except ftplib.error_perm:
        print('ERROR: cannot login')
        ftp.quit()
        raise
    print('*** Logged in successfully')

    try:
        ftp.cwd(dirname)
    except ftplib.error_perm:
        print(f'ERROR: cannot CD to {dirname}')
        ftp.quit()
        raise
    print(f'*** Changed to folder: {dirname}')

    try:
        with open(save_on, 'wb') as file:
            print('Getting ' + filename)
            ftp.retrbinary('RETR ' + filename, file.write)
        print(f'File downloaded to {save_on}')
    except ftplib.error_perm :
        print(f'ERROR: cannot read file {filename}')
        raise

    ftp.quit()

    # Hora atual no formato YYYYMMDDHHmm
    current_time = pendulum.now('America/Sao_Paulo').strftime("%Y%m%d%H%M")

    return save_on, current_time


@task
def tratar_dados(filename: Union[str, Path]) -> pd.DataFrame:
    '''
    Renomeia colunas e filtra dados com a hora e minuto do timestamp
    de execução mais próximo à este
    '''
    colunas = ['id', 'estacao', 'localizacao', 'data_medicao', 'latitude', 'longitude',
                'acumulado_chuva_15_min', 'acumulado_chuva_1_h',
                'acumulado_chuva_4_h', 'acumulado_chuva_24_h',
                'acumulado_chuva_96_h', 'acumulado_chuva_mes']

    dados = pd.read_csv(filename, skiprows=1, sep=';', names=colunas, decimal=',')

    # Adequando formato de data
    dados['data_medicao'] = pd.to_datetime(dados['data_medicao'], format='%H:%M - %d/%m/%Y')


    # Ordenação de variáveis
    cols_order = [
                  'data_medicao',
                  'estacao',
                  'acumulado_chuva_15_min',
                  'acumulado_chuva_1_h',
                  'acumulado_chuva_4_h',
                  'acumulado_chuva_24_h',
                  'acumulado_chuva_96_h']

    dados = dados[cols_order]

    # Mantendo primeira letra maiúscula na variável categória
    dados['estacao'] = dados['estacao'].str.capitalize()

    # Alterando valores ND, '-' e np.nan para NULL
    dados.replace(['ND', '-', np.nan], [None, None, None], inplace=True)

    # Converte variáveis que deveriam ser float para float
    float_cols = ['acumulado_chuva_15_min',
                  'acumulado_chuva_1_h',
                  'acumulado_chuva_4_h',
                  'acumulado_chuva_24_h',
                  'acumulado_chuva_96_h']
    dados[float_cols] = dados[float_cols].apply(pd.to_numeric, errors='coerce')

    return dados


@task(nout=2)
def salvar_dados(dados: pd.DataFrame, current_time: str) -> Tuple[Union[str, Path], str]:
    '''
    Salvar dados em csv
    '''

    ano = current_time[:4]
    mes = str(int(current_time[4:6]))
    dia = str(int(current_time[6:8]))
    partitions = os.path.join(f'ano={ano}', f'mes={mes}', f'dia={dia}')

    base_path = os.path.join(os.getcwd(),
                             'data',
                             'precipitacao_alertario',
                             'output',
                             partitions)

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    filename = os.path.join(base_path, f'dados_{current_time}.csv')

    print(f'Saving {filename}')
    dados.to_csv(filename, index=False)
    return filename, partitions
