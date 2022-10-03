# -*- coding: utf-8 -*-

import datetime
import requests
import pandas as pd
import numpy as np
import time
from typing import Dict
from prefect import task

# EMD Imports #

from pipelines.utils.utils import log, get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

# Tasks #

custom_fields = {
    "Serviço:\n": "servico_recurso",
    "Linha:\n": "linha_recurso",
    "Número de ordem, se aplicável:\n": "id_veiculo",
    "Hora de Início da Viagem, se aplicável:\n": "datetime_partida",
    "Hora do fim da viagem, se aplicável:\n": "datetime_chegada",
    "Dia da Viagem:\n": "data",
    "Direção do Serviço:\n": "sentido",
}


@task
def create_api_url_recursos(
    date_range_start, date_range_end, skip, top=1000
) -> Dict:  # pylint: disable=E501
    """
    Returns movidesk URL to get the requests over date range.
    """
    date_range_start = pd.to_datetime(date_range_start, utc=True).strftime(
        "%Y-%m-%dT%H:%M:%S.%MZ"
    )
    date_range_end = pd.to_datetime(date_range_end, utc=True).strftime(
        "%Y-%m-%dT%H:%M:%S.%MZ"
    )

    url = constants.SUBSIDIO_SPPO_RECURSO_API_BASE_URL.value
    log("URL Base: ", url)

    token = get_vault_secret(constants.SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH.value)[
        "data"
    ]["token"]
    url += f"token={token}"

    status = " or ".join(
        [f"baseStatus eq '{s}'" for s in ["New", "InAttendance", "Resolved"]]
    )
    dt = date_range_start
    de = date_range_end

    filters = f"category eq 'Recurso' and createdDate ge {dt} and createdDate lt {de}"

    params = {
        "select": "id,protocol,createdDate,baseStatus,servicefull",
        "filter": f"({filters}) and ({status})",
        "expand": "actions($select=id,description,htmlDescription,createdDate),customFieldValues($expand=items)",
        "orderby": "createdDate%20desc",
        "top": top,
        "skip": skip,
    }
    log("URL Parameters: ", params)

    for k, v in params.items():
        url += f"&${k}={v}"

    return url


@task
def get_raw(
    date_range_start: datetime, date_range_end: datetime, top: int = 1000
) -> Dict:
    """
    Returns a dataframe with all recurso data from movidesk api until date.
    """
    exit = False
    skip = 0
    df = pd.DataFrame()

    while not exit:
        # captura recursos da api
        try:
            url = create_api_url_recursos(date_range_start, date_range_end, skip)
            r = requests.get(url)
            r = r.json()
        except Exception as error:
            return {"data": pd.DataFrame(), "error": error}  # TODO: add retry policy

        for recurso in r:
            # filtra recurso de viagem individual
            if recurso["serviceFull"] == [
                "SPPO",
                "Viagem Individual - Recurso Viagens Subsídio",
            ]:
                # busca acoes do recurso
                for action in recurso["actions"]:
                    row = {
                        "data_recurso": action["createdDate"],
                    }
                # recupera dados de viagem informados no recurso
                row.update(
                    {
                        "id_recurso": recurso["protocol"],
                        "status_recurso": recurso["baseStatus"],
                        "id_veiculo": recurso["customFieldValues"][7]["value"],
                        "data": recurso["customFieldValues"][1]["value"],
                        "datetime_partida": recurso["customFieldValues"][2]["value"],
                        "datetime_chegada": recurso["customFieldValues"][3]["value"],
                        "servico": recurso["customFieldValues"][5]["value"],
                        "tipo_servico": recurso["customFieldValues"][6]["items"][0][
                            "customFieldItem"
                        ],
                        "sentido": recurso["customFieldValues"][8]["items"][0][
                            "customFieldItem"
                        ],
                    }
                )

                # salva apenas ultimo dado de viagem informado no recurso
                df = df.append(row, ignore_index=True)
        # itera os proximos recursos do periodo
        if len(r) == top:
            skip += top
            time.sleep(6)
        else:
            exit = True

    # print(df.head())
    if len(df) == 0:
        return {"data": list(), "erro": "Empty data"}
    return {"data": df.set_index("id_recurso", drop=True), "error": None}


# linha_recurso ja nao tem espaco, esta em uppercase
def split_servico(x):

    tipo_servico = ["A", "N", "V", "P", "R", "E", "D", "B", "C", "F", "G"]
    variacao_servico = ["A", "B", "C", "D"]

    if len(x) == 0:
        # print("1:", x)
        return "", ""
    # servico nao inicia com S
    if x[0] != "S":
        # print("2:", x[0])
        return "", ""
    # tipo servico invalido
    if x[1] not in tipo_servico:
        # print("3:", x[1])
        return "", ""
    if len(x) == 2:
        # print("4:", x)
        return x, ""
    # variacao servico invalida
    if x[2] not in variacao_servico:
        # print("5:", x[2])
        return "", ""
    if len(x) == 3:
        # print("6:", x)
        return x[:2], x[2]
    # padrao nao identificado
    return "", ""


def routes_type_cross_validation(df) -> pd.DataFrame:
    """
    Validate linha and servico informed with patterns for route names.
    """
    # selecionando linha_recurso no formato esperado
    df["linha"] = df["linha_recurso"].str.upper().str.replace(" ", "")

    # padroniza servico informado
    df["tipo_servico"] = (
        "S" + df["servico_recurso"].str.extract(r"(^[a-zA-Z])")
    ).replace("SA", "")

    # extrai o numero da linha_recurso
    df["numero_linha_recurso"] = df["linha"].str.extract(r"(LECD)").fillna("") + df[
        "linha_recurso"
    ].str.extract(r"(\d+)")
    # filtra numero de linha valida (ate 4 digitos)
    df["aux"] = df["linha_recurso"].str.extract(r"(\d+)")
    df = df[(df["aux"].str.len() >= 2) & (df["aux"].str.len() <= 4)].drop(columns="aux")

    # extrai servico da linha informada (caso tenha)
    df["servico_linha_recurso"] = (
        df["linha"]
        .str.split("-")
        .str[0]
        .str.replace("LECD", "")
        .str.extract(r"([a-zA-Z]+)")
    )
    # caso nao tenha servico na linha (e nao seja LECD), usa o tipo servico informado
    df["servico_linha_recurso"] = df["servico_linha_recurso"].fillna(df["tipo_servico"])
    # remove variacao das lecds (so aceito se for regular)
    df.loc[
        df["numero_linha_recurso"].str.startswith("LECD"),
        "servico_linha_recurso",
    ] = ""

    # verifica servico e variacao compostos na linha_recurso
    df["servico_linha_recurso"], df["variacao_servico_linha_recurso"] = zip(
        *df["servico_linha_recurso"].fillna("").apply(lambda x: split_servico(x))
    )

    # filtra servicos validos
    df = df[df["servico_linha_recurso"] == df["tipo_servico"]]
    df["servico"] = (
        df["tipo_servico"]
        + df["variacao_servico_linha_recurso"]
        + df["numero_linha_recurso"]
    )

    return df.drop(
        columns=[
            "linha",
            "tipo_servico",
            "variacao_servico_linha_recurso",
            "numero_linha_recurso",
            "servico_linha_recurso",
        ]
    )


# @task
def pre_treatment_subsidio_sppo_recursos(status: dict, timestamp: datetime) -> Dict:
    """
    Treat recursos requested from movidesk api.
    """
    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    if status["data"] == []:
        # log("Data is empty, skipping treatment...")
        return {"data": pd.DataFrame(), "error": status["error"]}

    df = status["data"]

    # filtrando não nulos
    df = df.dropna()

    # seleciona número de ordem no formato esperado
    df = df[df["id_veiculo"].str.match(pat=r"(^\d{5})")]

    # selecionando data no formato esperado
    df = df[df["data"].str.match(pat=r"(^\d{2}/\d{2}/\d{4}$)")]

    # seleciona datetime_chegada e datetime_partida no formato esperado
    df = df[df["datetime_partida"].str.match(pat=r"(^\d{2}:\d{2}$)")]
    df = df[df["datetime_chegada"].str.match(pat=r"(^\d{2}:\d{2}$)")]

    # validando data inicio < data fim e aplicando data type à colunas
    df["datetime_chegada"] = pd.to_datetime(
        df["data"].str.cat(df["datetime_chegada"], sep=" ")
    ).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["datetime_partida"] = pd.to_datetime(
        df["data"].str.cat(df["datetime_partida"], sep=" ")
    ).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["data_recurso"] = pd.to_datetime(df["data_recurso"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # verifica se a data de partida é valida
    df = df[df["datetime_partida"] < df["datetime_chegada"]]
    df = df[df["datetime_partida"] <= df["data_recurso"]]

    df["data"] = pd.to_datetime(df["data"]).dt.strftime("%Y-%m-%d")
    df["sentido"] = df["sentido"].map({"Ida": "I", "Volta": "V", "Circular": "C"})

    # verifica linha e servico informados
    for col in ["servico_recurso", "linha_recurso"]:
        df[col] = df[col].str.replace("\xa0", " ").str.replace("\n", "")
    df = routes_type_cross_validation(df)

    return {"data": df, "error": None}
