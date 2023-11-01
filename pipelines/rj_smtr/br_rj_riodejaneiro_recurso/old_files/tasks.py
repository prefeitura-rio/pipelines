# -*- coding: utf-8 -*-
import time
import pandas as pd
from prefect import task
from typing import Dict
import json

# EMD Imports #

from pipelines.utils.utils import log

# SMTR Imports #

from pipelines.rj_smtr.br_rj_riodejaneiro_recurso.utils import (
    create_api_url_recursos,
    request_data,
    trata_motivo,
    analisar_ja_julgados,
    analisar_linha,
)

# Tasks #


@task
def get_raw_recursos(date_range_end: str, skip: int = 0, top: int = 1000) -> Dict:
    """
    Returns a dataframe with recursos data from movidesk api.
    """
    all_records = False
    status = {"data": [], "error": None}

    while not all_records:
        url = create_api_url_recursos(date_range_end, skip)
        current_status = request_data(url)

        if not current_status or current_status.get("error"):
            raise ValueError(
                f"O request de {url} falhou. Como precisaremos de \
                    todos os dados para prosseguir, encerraremos o script."
            )

        elif current_status.get("error") and current_status.get("data") is None:
            raise ValueError(
                "O request falhou sem pegar nenhum dado, encerraremos o script."
            )

        status["data"] += current_status.get("data")

        # itera os proximos recursos do periodo
        if len(current_status.get("data")) == top:
            skip += top
            time.sleep(5)
        else:
            all_records = True

    if len(status["data"]) == 0:
        raise ValueError("Nenhum dado para tratar.")

    # status = pd.DataFrame(status["data"])

    log(f"Request concluído, com status: {status}.")

    return status


@task
def pre_treatment_subsidio_sppo_recursos(status: dict, timestamp: str) -> Dict:
    """
    Treat recursos requested from movidesk api.
    """

    colunas = [
        "protocolo",
        "data_ticket",
        "id_recurso",
        "acao",
        "linha",
        "customFieldValues",
        "julgamento",
        "motivo",
        "json_motivo",
        "julgado_anteriormente",
        "julgamento_anterior",
    ]

    # Criar o DataFrame vazio com as colunas especificadas
    data = pd.DataFrame(columns=colunas)

    log(f"Status: {type(status)}")

    for index, item in status.iterrows():
        # rename ao invés iterrows
        row = {
            "protocolo": item["protocolo"],
            "data_ticket": item["createdDate"],
            "id_recurso": item["id"],
            "acao": item["actions"],
            "customFieldValues": item["customFieldValues"],
            "julgamento": item["julgamento"],
            "motivo": item["motivo"],
            "json_motivo": dict,
            "julgado_anteriormente": False,
            "julgamento_anterior": None,
        }

        row["motivo"] = trata_motivo(row["motivo"])
        row = analisar_ja_julgados(row)
        row = analisar_linha(row)

        data.loc[len(data)] = row

        data.rename(columns={"protocol": "protocolo"}, inplace=True)

        log(f"Ticket: {row['protocol']} tratado com sucesso!")

    data["timestamp_captura"] = timestamp

    return data
