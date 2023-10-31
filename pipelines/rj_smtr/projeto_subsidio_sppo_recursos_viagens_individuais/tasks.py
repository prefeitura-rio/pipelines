# -*- coding: utf-8 -*-
import time
import pandas as pd
from prefect import task

# EMD Imports #

from pipelines.utils.utils import log

# SMTR Imports #

from pipelines.rj_smtr.projeto_subsidio_sppo_recursos_viagens_individuais.utils import (
    create_api_url_recursos,
    request_data,
    trata_motivo,
    analisar_ja_julgados,
    analisar_linha,
)

# Tasks #


@task
def get_raw_recursos(
    date_range_end: str, skip: int = 0, top: int = 1000
) -> pd.DataFrame:
    """
    Returns a dataframe with recursos data from movidesk api.
    """
    all_records = False
    status = {"data": [], "error": None}

    while not all_records:
        url = create_api_url_recursos(date_range_end, skip)
        current_status = request_data(url)

        if not current_status or current_status["error"]:
            raise ValueError(
                f"O request de {url} falhou. Como precisaremos de \
                todos os dados para prosseguir, encerraremos o script."
            )

        elif current_status["error"] and current_status["data"] is None:
            raise ValueError(
                "O request falhou sem pegar nenhum dado, encerraremos o script."
            )

        status["data"] += current_status["data"]

        # log(f"Status: {status}")

        # itera os proximos recursos do periodo
        if len(current_status["data"]) == top:
            skip += top
            time.sleep(6)
        else:
            all_records = True

    if len(status["data"]) == 0:
        raise ValueError("Nenhum dado para tratar.")

    status = pd.DataFrame(status["data"])
    log(f"Request concluído, com status: {status}.")

    path = status.to_csv("recursos_registados_movidesk.csv", index=None)

    log(f"Arquivo salvo em {path}")
    return status


@task
def pre_treatment_subsidio_sppo_recursos(status, timestamp: str) -> pd.DataFrame:
    """
    Treat recursos requested from movidesk api.
    """

    colunas = [
        "protocolo",
        "data_ticket",
        "id_recurso",
        "acao",
        "customFieldValues",
        "julgamento",
        "motivo",
        "json_motivo",
        "json_observacao",
    ]

    # Criar o DataFrame vazio com as colunas especificadas
    data = pd.DataFrame(columns=colunas)

    for index, item in status.iterrows():
        row = {
            "protocolo": item["protocolo"],
            "data_ticket": item["createdDate"],
            "id_recurso": item["id"],
            "acao": item["actions"],
            "customFieldValues": eval(item["customFieldValues"]),
            "julgamento": item["julgamento"],
            "motivo": item["motivo"],
            "observacao": item["observacao"],
            "json_motivo": dict,
        }

        row["motivo"] = trata_motivo(row["motivo"])
        row = analisar_ja_julgados(row)
        row = analisar_linha(row)

        data.loc[len(data)] = row
        log(f"Ticket: {row['protocolo']} tratado com sucesso!")

        data["timestamp_captura"] = timestamp

    return data
