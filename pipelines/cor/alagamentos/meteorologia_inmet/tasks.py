"""
Tasks for meteorologia_inmet
"""
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Tuple, Union

import basedosdados as bd
import pandas as pd
import pendulum
from prefect import task
import requests

from pipelines.utils import log
#from pipelines.cor.alagamentos.meteorologia_inmet.utils import main, save_parquet

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
    return data, hora

@task
def download(data: str) -> pd.DataFrame:
    '''
    Faz o request na data especificada e retorna dados
    '''

    # Lista com as estações da cidade do Rio de Janeiro
    estacoes_unicas = ['A602', 'A621', 'A636', 'A651', 'A652', 'A653', 'A654', 'A655',
       'A656']

    # Faz o request do dia atual e salva na variável raw
    raw = []
    for id_estacao in estacoes_unicas:
        url = f"https://apitempo.inmet.gov.br/estacao/{data}/{data}/{id_estacao}"
        res = requests.get(url)
        if res.status_code!=200:
            print('Problema no id: ', id_estacao, '\n', res.status_code, '\n', url)
            continue
        raw.append(json.loads(res.text))

    # Faz um flat da lista de listas
    #flat_list = [item for sublist in raw for item in sublist]
    #raw = flat_list.copy()

    # remove da lista colunas que não serão interessantes
    # pois já tem essa informação na tabela estacoes_inmet
    drop_cols = ['DC_NOME', 'VL_LATITUDE', 'VL_LONGITUDE', 'TEM_SEN', 'UF']
    for i, j in enumerate(raw):
        print('\n>>>', j)
        raw[i] = {key: j[0][key] for key in j[0] if key not in drop_cols}

    # converte para dados
    dados = pd.DataFrame(raw)

    return dados

@task
def tratar_dados(dados: pd.DataFrame, hora: str) -> pd.DataFrame:
    '''
    Renomeia colunas e filtra dados com a hora do timestamp de execução
    '''
    # Adequando nome das variáveis
    rename_cols = {
        'DC_NOME':'estacao',
        'UF':'sigla_uf',
        'VL_LATITUDE':'latitude',
        'VL_LONGITUDE':'longitude',
        'CD_ESTACAO':'id_estacao',
        'VEN_DIR':'direcao_vento',
        'DT_MEDICAO': 'data',
        'HR_MEDICAO': 'horario',
        'VEN_RAJ':'rajada_vento_max',
        'CHUVA':'acumulado_chuva_1_h',
        'PRE_INS':'pressao',
        'PRE_MIN':'pressao_minima',
        'PRE_MAX':'pressao_maxima',
        'UMD_INS':'umidade',
        'UMD_MIN':'umidade_minima',
        'UMD_MAX':'umidade_maxima',
        'VEN_VEL':'velocidade_vento',
        'TEM_INS':'temperatura',
        'TEM_MIN':'temperatura_minima',
        'TEM_MAX':'temperatura_maxima',
        'RAD_GLO':'radiacao_global',
        'PTO_INS':'temperatura_orvalho',
        'PTO_MIN':'temperatura_orvalho_minimo',
        'PTO_MAX':'temperatura_orvalho_maximo'}

    dados = dados.rename(columns=rename_cols)

    # Seleciona apenas dados daquela hora
    dados = dados[dados['horario']==str(hora)+'00']

    # Converte coluna de horas de 2300 para 23:00:00
    dados['horario'] = pd.to_datetime(dados.horario,format='%H%M')
    dados['horario'] = dados.horario.apply(lambda x: datetime.strftime(x,'%H:%M:%S'))

    # Ordenamento de variáveis
    chaves_primarias = ['id_estacao', 'data', 'horario']
    demais_cols = [c for c in dados.columns if c not in chaves_primarias]

    dados = dados[chaves_primarias+demais_cols]

    # Converte variáveis que deveriam ser float para float
    float_cols = ['pressao', 'pressao_maxima',
       'radiacao_global', 'temperatura_orvalho', 'temperatura_minima',
       'umidade_minima', 'temperatura_orvalho_maximo', 'direcao_vento',
       'acumulado_chuva_1_h', 'pressao_minima', 'umidade_maxima',
       'velocidade_vento', 'temperatura_orvalho_minimo', 'temperatura_maxima',
       'rajada_vento_max', 'temperatura', 'umidade']
    dados[float_cols] = dados[float_cols].astype(float)

    dados['horario'] = pd.to_datetime(dados.horario,format='%H:%M:%S').dt.time
    dados['data'] = pd.to_datetime(dados.data,format='%Y-%m-%d')

    return dados

@task
def salvar_dados(dados: pd.DataFrame) -> Union[str, Path]:
    '''
    Salvar dados em csv
    '''
    base_path = os.path.join(os.getcwd(),'data', 'meteorologia_inmet', 'output')

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
