# -*- coding: utf-8 -*-
"""
Tasks for meteorologia_redemet
"""
from datetime import timedelta
import json
from pathlib import Path
from typing import Tuple, Union

import pandas as pd
import pendulum
from prefect import task
import requests

from pipelines.constants import constants
from pipelines.utils.utils import (
    get_vault_secret,
    log,
    to_partitions,
    parse_date_columns,
)


@task(nout=3)
def get_dates(data_inicio: str, data_fim: str) -> Tuple[str, str]:
    """
    Task para obter o dia de início e o de fim.
    Se nenhuma data foi passada a data_inicio corresponde a ontem
    e data_fim a hoje e não estamos fazendo backfill.
    Caso contrário, retorna as datas inputadas mos parâmetros do flow.
    """
    # a API sempre retorna o dado em UTC
    log(f"data de inicio e fim antes do if {data_inicio} {data_fim}")
    if data_inicio == "":
        data_fim = pendulum.now("UTC").format("YYYY-MM-DD")
        data_inicio = pendulum.yesterday("UTC").format("YYYY-MM-DD")
        backfill = 0
    else:
        backfill = 1
    log(f"data de inicio e fim dps do if {data_inicio} {data_fim}")

    return data_inicio, data_fim, backfill


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download(data_inicio: str, data_fim: str) -> pd.DataFrame:
    """
    Faz o request na data especificada e retorna dados
    """

    # Lista com as estações da cidade do Rio de Janeiro
    estacoes_unicas = [
        "SBAF",
        "SBGL",
        "SBJR",
        "SBRJ",
        "SBSC",
    ]

    dicionario = get_vault_secret("redemet-token")

    # Converte datas em int para cálculo de faixas.
    data_inicio_int = int(data_inicio.replace("-", ""))
    data_fim_int = int(data_fim.replace("-", ""))

    raw = []
    for id_estacao in estacoes_unicas:
        base_url = f"https://api-redemet.decea.mil.br/aerodromos/info?api_key={dicionario['data']['token']}"  # noqa
        for data in range(data_inicio_int, data_fim_int + 1):
            for hora in range(24):
                url = f"{base_url}&localidade={id_estacao}&datahora={data:06}{hora:02}"
                res = requests.get(url)
                if res.status_code != 200:
                    log(f"Problema no id: {id_estacao}, {res.status_code}, {url}")
                    continue
                res_data = json.loads(res.text)
                if res_data["status"] is not True:
                    log(f"Problema no id: {id_estacao}, {res_data['message']}, {url}")
                    continue
                if "data" not in res_data["data"]:
                    # Sem dados para esse horario
                    continue
                raw.append(res_data)

    # Extrai objetos de dados
    raw = [res_data["data"] for res_data in raw]

    # converte para dados
    dados = pd.DataFrame(raw)

    return dados


@task
def tratar_dados(dados: pd.DataFrame, backfill: bool = 0) -> pd.DataFrame:
    """
    Renomeia colunas e filtra dados com a hora do timestamp de execução
    """

    drop_cols = ["nome", "cidade", "lon", "lat", "localizacao", "tempoImagem", "metar"]
    # Checa se todas estão no df
    drop_cols = [c for c in drop_cols if c in dados.columns]

    # Remove colunas que já temos os dados em outras tabelas
    dados = dados.drop(drop_cols, axis=1)

    # Adequando nome das variáveis
    rename_cols = {
        "localidade": "id_estacao",
        "ur": "umidade",
    }

    dados = dados.rename(columns=rename_cols)

    # Converte horário de UTC para America/Sao Paulo
    formato = "DD/MM/YYYY HH:mm(z)"
    dados["data"] = dados["data"].apply(
        lambda x: pendulum.from_format(x, formato)
        .in_tz("America/Sao_Paulo")
        .format(formato)
    )

    # Ordenamento de variáveis
    chaves_primarias = ["id_estacao", "data"]
    demais_cols = [c for c in dados.columns if c not in chaves_primarias]

    dados = dados[chaves_primarias + demais_cols]

    # Converte variáveis que deveriam ser int para int
    dados["temperatura"] = dados["temperatura"].apply(
        lambda x: None if x[:-2] == "NIL" else int(x[:-2])
    )
    dados["umidade"] = dados["umidade"].apply(
        lambda x: None if "%" not in x else int(x[:-1])
    )

    dados["data"] = pd.to_datetime(dados.data, format="%d/%m/%Y %H:%M(%Z)")

    # Pegar o dia no nosso timezone como partição
    br_timezone = pendulum.now("America/Sao_Paulo").format("YYYY-MM-DD")

    # Define colunas que serão salvas
    dados = dados[
        [
            "id_estacao",
            "data",
            "temperatura",
            "umidade",
            "condicoes_tempo",
            "ceu",
            "teto",
            "visibilidade",
        ]
    ]

    # Remover dados duplicados
    dados = dados.drop_duplicates(subset=["id_estacao", "data"])

    log(f"Dados antes do filtro dia:\n{dados[['id_estacao', 'data']]}")

    if not backfill:
        # Seleciona apenas dados daquele dia (devido à UTC)
        dados = dados[dados["data"].dt.date.astype(str) == br_timezone]

    log(f">>>> min hora {dados[~dados.temperatura.isna()].data.min()}")
    log(f">>>> max hora {dados[~dados.temperatura.isna()].data.max()}")

    # Remover fuso horário
    dados["data"] = dados["data"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Capitalizar os dados da coluna céu
    dados["ceu"] = dados["ceu"].str.capitalize()

    return dados


@task
def salvar_dados(dados: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados em csv
    """

    prepath = Path("/tmp/meteorologia_redemet/")
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data"
    dataframe, partitions = parse_date_columns(dados, partition_column)

    # Cria partições a partir da data
    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
    )
    log(f"[DEBUG] Files saved on {prepath}")
    return prepath


@task
def tratar_dados_estacao(data_inicio: str, data_fim: str) -> pd.DataFrame:
    # Lista com as estações da cidade do Rio de Janeiro
    estacoes_unicas = [
        "SBAF",
        "SBGL",
        "SBJR",
        "SBRJ",
        "SBSC",
    ]

    dicionario = get_vault_secret("redemet-token")

    # Converte datas em int para cálculo de faixas.
    data_inicio_int = int(data_inicio.replace("-", ""))
    data_fim_int = int(data_fim.replace("-", ""))

    raw = []
    for id_estacao in estacoes_unicas:
        base_url = f"https://api-redemet.decea.mil.br/aerodromos/info?api_key={dicionario['data']['token']}"  # noqa
        for data in range(data_inicio_int, data_fim_int + 1):
            for hora in range(24):
                url = f"{base_url}&localidade={id_estacao}&datahora={data:06}{hora:02}"
                res = requests.get(url)
                if res.status_code != 200:
                    log(f"Problema no id: {id_estacao}, {res.status_code}, {url}")
                    continue
                res_data = json.loads(res.text)
                if res_data["status"] is not True:
                    log(f"Problema no id: {id_estacao}, {res_data['message']}, {url}")
                    continue
                if "data" not in res_data["data"]:
                    # Sem dados para esse horario
                    continue
                raw.append(res_data)

    # Função para converter longitude de graus, minutos, segundos para decimal
    res_data["latitude"] = res_data["lat"].apply(converter_lat_lon)
    res_data["longitude"] = res_data["lon"].apply(converter_lat_lon)
    return dados
    
    
    
def converter_lat_lon(longitude_str):
    longitude_str = longitude_str.replace("º", "/").replace("''", "/").replace("'", "/")
    
    # Divida a string com base nos espaços em branco
    partes = longitude_str.split("/")
    # print(partes)
    
    # Extraia os graus, minutos e segundos da lista de partes
    graus = int(partes[0])
    minutos = int(partes[1])
    segundos = float(partes[2])

    # Calcule o valor decimal
    decimal = graus + (minutos / 60) + (segundos / 3600)

    # Verifique se a direção é Oeste (W) e faça o valor negativo
    # Verifique se a direção é Norte (N) e retorne o valor decimal
    if ("W" in partes[3].upper()) | ("S" in partes[3].upper()):
        decimal = -decimal
    
    return decimal