# -*- coding: utf-8 -*-
"""
Tasks for meteorologia_inmet
"""
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from typing import Tuple, Union

import pandas as pd
import pendulum
from prefect import task
import requests

from pipelines.constants import constants
from pipelines.utils.utils import log

# from pipelines.rj_cor.meteorologia.meteorologia_inmet.meteorologia_utils import converte_timezone


@task(nout=2)
def get_dates() -> Tuple[str, str]:
    """
    Task para obter o dia atual e o anterior
    """
    # segundo o manual do inmet o dado vem em UTC
    current_time = pendulum.now("UTC").format("YYYY-MM-DD")
    yesterday = pendulum.yesterday("UTC").format("YYYY-MM-DD")
    return current_time, yesterday


@task()
def slice_data(current_time: str) -> str:
    """
    Retorna a data e hora do timestamp de execução
    """
    if not isinstance(current_time, str):
        current_time = current_time.to_datetime_string()

    data = current_time[:10]
    return data


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download(data: str, yesterday: str) -> pd.DataFrame:
    """
    Faz o request na data especificada e retorna dados
    """

    # Lista com as estações da cidade do Rio de Janeiro
    estacoes_unicas = [
        "A602",
        "A621",
        "A636",
        "A651",
        "A652",
        "A653",
        "A654",
        "A655",
        "A656",
    ]

    # Faz o request do dia atual e anterior e salva na variável raw
    # Trazer desde o dia anterior evita problemas quando já é outro dia
    # no UTC, visto que ele só traria dados do novo dia e substituiria
    # no arquivo da partição do dia atual no nosso timezone
    raw = []
    for id_estacao in estacoes_unicas:
        url = f"https://apitempo.inmet.gov.br/estacao/{yesterday}/{data}/{id_estacao}"
        res = requests.get(url)
        if res.status_code != 200:
            log(
                f"Problema no id: {id_estacao}, {res.status_code}, {url}"
            )
            continue
        raw.append(json.loads(res.text))

    # Faz um flat da lista de listas
    flat_list = [item for sublist in raw for item in sublist]
    raw = flat_list.copy()

    # converte para dados
    dados = pd.DataFrame(raw)

    return dados


@task(nout=2)
def tratar_dados(dados: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    Renomeia colunas e filtra dados com a hora do timestamp de execução
    """

    def converte_timezone(data: str, horario: str) -> str:
        """
        Recebe o formato de data em YYYY-MM-DD e hora em HH:mm:SS no UTC
        e retorna no mesmo formato no horário São Paulo
        """
        datahora = pendulum.from_format(data + " " + horario, "YYYY-MM-DD HH:mm:SS")
        datahora = datahora.in_tz("America/Sao_Paulo")

        data = datahora.format("YYYY-MM-DD")
        horario = datahora.format("HH:mm:SS")
        return data, horario

    drop_cols = ["DC_NOME", "VL_LATITUDE", "VL_LONGITUDE", "TEM_SEN", "UF", 'TEN_BAT', 'TEM_CPU']
    # Checa se todas estão no df
    drop_cols = [c for c in drop_cols if c in dados.columns]

    # Remove colunas que já temos os dados em outras tabelas
    dados = dados.drop(drop_cols, axis=1)

    # Adequando nome das variáveis
    rename_cols = {
        "DC_NOME": "estacao",
        "UF": "sigla_uf",
        "VL_LATITUDE": "latitude",
        "VL_LONGITUDE": "longitude",
        "CD_ESTACAO": "id_estacao",
        "VEN_DIR": "direcao_vento",
        "DT_MEDICAO": "data",
        "HR_MEDICAO": "horario",
        "VEN_RAJ": "rajada_vento_max",
        "CHUVA": "acumulado_chuva_1_h",
        "PRE_INS": "pressao",
        "PRE_MIN": "pressao_minima",
        "PRE_MAX": "pressao_maxima",
        "UMD_INS": "umidade",
        "UMD_MIN": "umidade_minima",
        "UMD_MAX": "umidade_maxima",
        "VEN_VEL": "velocidade_vento",
        "TEM_INS": "temperatura",
        "TEM_MIN": "temperatura_minima",
        "TEM_MAX": "temperatura_maxima",
        "RAD_GLO": "radiacao_global",
        "PTO_INS": "temperatura_orvalho",
        "PTO_MIN": "temperatura_orvalho_minimo",
        "PTO_MAX": "temperatura_orvalho_maximo",
    }

    dados = dados.rename(columns=rename_cols)

    # Converte coluna de horas de 2300 para 23:00:00
    dados["horario"] = pd.to_datetime(dados.horario, format="%H%M")
    dados["horario"] = dados.horario.apply(lambda x: datetime.strftime(x, "%H:%M:%S"))

    # Converte horário de UTC para America/Sao Paulo
    dados[["data", "horario"]] = dados[["data", "horario"]].apply(
        lambda x: converte_timezone(x.data, x.horario), axis=1, result_type="expand"
    )

    # Ordenamento de variáveis
    chaves_primarias = ["id_estacao", "data", "horario"]
    demais_cols = [c for c in dados.columns if c not in chaves_primarias]

    dados = dados[chaves_primarias + demais_cols]

    # Converte variáveis que deveriam ser float para float
    float_cols = [
        "pressao",
        "pressao_maxima",
        "radiacao_global",
        "temperatura_orvalho",
        "temperatura_minima",
        "umidade_minima",
        "temperatura_orvalho_maximo",
        "direcao_vento",
        "acumulado_chuva_1_h",
        "pressao_minima",
        "umidade_maxima",
        "velocidade_vento",
        "temperatura_orvalho_minimo",
        "temperatura_maxima",
        "rajada_vento_max",
        "temperatura",
        "umidade",
    ]
    dados[float_cols] = dados[float_cols].astype(float)

    dados["horario"] = pd.to_datetime(dados.horario, format="%H:%M:%S").dt.time
    dados["data"] = pd.to_datetime(dados.data, format="%Y-%m-%d")

    # Pegar o dia no nosso timezone como partição
    br_timezone = pendulum.now("America/Sao_Paulo").format("YYYY-MM-DD")
    ano = br_timezone[:4]
    mes = str(int(br_timezone[5:7]))
    dia = str(int(br_timezone[8:10]))

    # Define colunas que serão salvas
    dados = dados[[
        'id_estacao', 'data', 'horario', 'pressao', 'pressao_maxima',
        'radiacao_global', 'temperatura_orvalho', 'temperatura_minima',
        'umidade_minima', 'temperatura_orvalho_maximo', 'direcao_vento',
        'acumulado_chuva_1_h', 'pressao_minima', 'umidade_maxima',
        'velocidade_vento', 'temperatura_orvalho_minimo', 'temperatura_maxima',
        'rajada_vento_max', 'temperatura', 'umidade'
    ]]

    # Seleciona apenas dados daquele dia (devido à UTC)
    dados = dados[dados["data"] == br_timezone]

    # Remove linhas com todos os dados nan
    dados = dados.dropna(subset=float_cols, how='all')

    partitions = f"ano={ano}/mes={mes}/dia={dia}"
    log(f">>> partitions{partitions}")
    print(">>>> max hora ", dados[~dados.temperatura.isna()].horario.max())
    return dados, partitions


@task
def salvar_dados(dados: pd.DataFrame, partitions: str, data: str) -> Union[str, Path]:
    """
    Salvar dados em csv
    """
    base_path = Path(os.getcwd(), "data", "meteorologia_inmet", "output")

    partition_path = Path(base_path, partitions)

    if not os.path.exists(partition_path):
        os.makedirs(partition_path)

    filename = str(partition_path / f"dados_{data}.csv")

    log(f"Saving {filename}")
    # dados.to_csv(filename, index=False)
    dados.to_csv(r"{}".format(filename), index=False)
    return base_path
