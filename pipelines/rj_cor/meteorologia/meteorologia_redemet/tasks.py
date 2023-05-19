# -*- coding: utf-8 -*-
"""
Tasks for meteorologia_redemet
"""
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Tuple, Union

import pandas as pd
import pendulum
from prefect import task
import requests

from pipelines.constants import constants
from pipelines.utils.utils import get_vault_secret, log, to_partitions
from pipelines.rj_cor.meteorologia.precipitacao_alertario.utils import (
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
    token = dicionario["data"]["token"]

    # Converte datas em int para cálculo de faixas.
    data_inicio_int = int(data_inicio.replace("-", ""))
    data_fim_int = int(data_fim.replace("-", ""))

    raw = []
    for id_estacao in estacoes_unicas:
        base_url = f"https://api-redemet.decea.mil.br/aerodromos/info?api_key={token}"
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

    drop_cols = [
        "nome",
        "cidade",
        "lon",
        "lat",
        "localizacao",
        "tempoImagem",
    ]
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
    dados["temperatura"] = dados["temperatura"].apply(lambda x: int(x[:-2]))
    dados["umidade"] = dados["umidade"].apply(lambda x: int(x[:-1]))

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

    if not backfill:
        # Seleciona apenas dados daquele dia (devido à UTC)
        dados = dados[dados["data"] == br_timezone]

    log(f">>>> max hora {dados[~dados.temperatura.isna()].data.max()}")
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
