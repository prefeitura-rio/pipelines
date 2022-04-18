"""
Tasks for precipitacao_alertario
"""

from datetime import datetime
import json
import math
import os
from pathlib import Path
from typing import Tuple, Union

import basedosdados as bd
import numpy as np
import pandas as pd
import pendulum
from prefect import task
import requests

from pipelines.utils import log

###################################################################################
######## Ver de trocar o nome da coluna data_medição e deixar padronizado #########
###################################################################################

def nearest_floor_multiple(base:int, number: int) -> int:
    """
    Round `a_number` to a multiple of `base` considering the
    floor on base

    base: int
    just multiples of this base are considered
    number: int
    number to be approximated

    Returns:
        _type_: _description_
    """
    return base * math.floor(number/base)

@task(nout=2)
def slice_data(current_time: str) -> Tuple[str, str]:
    '''
    Retorna a data e hora do timestamp de execução
    '''
    if not isinstance(current_time,str):
        current_time = current_time.to_datetime_string()

    current_time = current_time or pendulum.now("utc").to_datetime_string()

    data = current_time[:10]
    hora = current_time[11:13]
    minuto = current_time[14:16]

    # Não importa que minuto rodar (se atrasou alguns minutos
    # por exemplo) quero que traga o dado imediatamente anterior
    # consierando que a informação é atualizada a cada 5 min
    minuto = nearest_floor_multiple(5, minuto)
    return data, hora, minuto

@task
def download(data: str) -> pd.DataFrame:
    '''
    Faz o request na data especificado e retorna dados
    Tentando conseguir com o sardinha o ftp que permite
    termos os dados atualizados a cada 5 minutos
    '''
    dados=0
    pass

    return dados

@task
def tratar_dados(dados: pd.DataFrame, hora: str) -> pd.DataFrame:
    '''
    Renomeia colunas e filtra dados com a hora e minuto do timestamp
    de execução mais próximo à este 
    '''
    # Adequando nome das variáveis
    rename_cols = {'ts':'data_medicao',
                   'acumulado_15_min':'acumulado_chuva_15_min',
                   'acumulado_1_h':'acumulado_chuva_1_h',
                   'acumulado_4_h':'acumulado_chuva_4_h',
                   'acumulado_24_h':'acumulado_chuva_24_h',
                   'acumulado_96_h':'acumulado_chuva_96_h'
                    }

    dados = dados.rename(columns=rename_cols)

    # Ordenação de variáveis
    cols_order = ['ano', 
                  'mes', 
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

@task
def salvar_dados(dados: pd.DataFrame) -> Union[str, Path]:
    '''
    Salvar dados em csv
    '''
    base_path = os.path.join(os.getcwd(),'data', 'precipitacao_alertario', 'output')

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    filename = os.path.join(base_path, 'dados.csv')

    print(f'Saving {filename}')
    dados.to_csv(filename, index=False)
    return filename

@task
def upload_to_gcs(path: Union[str, Path], dataset_id: str, table_id: str) -> None:
    """
    Uploads a bunch of CSVs using BD+
    """
    table = bd.Table(dataset_id=dataset_id, table_id=table_id)
    _ = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    if table.table_exists(mode="staging"):
        # Delete old data
        # st.delete_table(
        #     mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        # log(
        #     f"Successfully deleted OLD DATA {st.bucket_name}.staging.{dataset_id}.{table_id}"
        # )

        # the name of the files need to be the same or the data doesn't get overwritten
        table.append(
            filepath=path,
            if_exists="append",
        )

        log(
            f"Successfully uploaded {path} to {table.bucket_name}.staging.{dataset_id}.{table_id}"
        )

    else:
        log(
            "Table does not exist in STAGING, need to create it in local first.\nCreate\
                 and publish the table in BigQuery first."
        )
