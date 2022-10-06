# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

# pylint: disable=too-many-arguments,broad-except,too-many-locals

# import datetime
from typing import Dict, List
import time
import requests
import pandas as pd
from prefect import task

# EMD Imports #

from pipelines.utils.utils import log, get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

# Tasks #


def create_api_url_recursos(date_range_start, date_range_end, skip=0, top=1000) -> Dict:
    """
    Returns movidesk URL to get the requests over date range.
    """
    url = constants.SUBSIDIO_SPPO_RECURSO_API_BASE_URL.value
    token = get_vault_secret(constants.SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH.value)[
        "data"
    ]["token"]

    base_status = " or ".join(
        [f"baseStatus eq '{s}'" for s in ["New", "InAttendance", "Resolved", "Closed"]]
    )
    status = "status ne 'Resolvido Automaticamente'"

    start = pd.to_datetime(date_range_start, utc=True).strftime("%Y-%m-%dT%H:%M:%S.%MZ")
    end = pd.to_datetime(date_range_end, utc=True).strftime("%Y-%m-%dT%H:%M:%S.%MZ")

    category = "category eq 'Recurso'"
    dates = f"createdDate ge {start} and createdDate lt {end}"
    service = "serviceFirstLevel eq 'Viagem Individual - Recurso Viagens Subsídio'"

    params = {
        "select": "id,protocol,createdDate,status,serviceSecondLevel,customFieldValues",
        "filter": f"({category} and {dates} and {service} and {status}) and ({base_status})",
        "expand": "customFieldValues($expand=items)",
        "orderby": "createdDate%20desc",
        "top": top,
        "skip": skip,
    }

    log(f"URL Parameters: {params}")

    url += f"token={token}"

    for param, value in params.items():
        url += f"&${param}={value}"

    return url


def request_data(url: str, headers: dict = None) -> Dict:
    """
    Return response json and error.
    """
    data = None

    # Get data from API
    try:
        response = requests.get(
            url, headers=headers, timeout=constants.MAX_TIMEOUT_SECONDS.value
        )
        error = None
    except Exception as exp:
        error = exp
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")
        return {"data": None, "error": error}

    # Check data results
    if response.ok:  # status code is less than 400
        data = response.json()

    return {"data": data, "error": error}


@task
def get_raw_recursos(
    date_range_start: str, date_range_end: str, skip: int = 0, top: int = 1000
) -> Dict:
    """
    Returns a dataframe with all recurso data from movidesk api until date.
    """
    all_records = False
    status = {"data": [], "error": None}

    while not all_records:
        url = create_api_url_recursos(date_range_start, date_range_end, skip)
        current_status = request_data(url)

        if current_status["error"] is not None:
            return {"data": None, "error": current_status["error"]}

        status["data"] += current_status["data"]

        # itera os proximos recursos do periodo
        if len(current_status["data"]) == top:
            skip += top
            time.sleep(6)
        else:
            all_records = True

    if len(status["data"]) == 0:
        status["error"] = "Empty data"

    return status


def get_custom_fields(custom_fields: List) -> Dict:
    """
    Return customFields dict.
    """
    map_field = {
        111867: "data_viagem",
        111868: "hora_partida",
        111869: "hora_chegada",
        111871: "servico",
        111873: "id_veiculo",
        111872: "tipo_servico",
        111901: "sentido",
    }

    row = {}
    for field in custom_fields:

        if field["customFieldId"] in map_field:
            # dropdown field
            if field["items"]:
                row.update(
                    {
                        map_field[field["customFieldId"]]: field["items"][0][
                            "customFieldItem"
                        ]
                    }
                )
            # other fields
            else:
                row.update({map_field[field["customFieldId"]]: field["value"]})

    return row


@task
def pre_treatment_subsidio_sppo_recursos(status: dict, timestamp: str) -> Dict:
    """
    Treat recursos requested from movidesk api.
    """
    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    if status["data"] == []:
        # log("Data is empty, skipping treatment...")
        return {"data": pd.DataFrame(), "error": status["error"]}

    data = pd.DataFrame()
    for item in status["data"]:
        row = {
            "modo": item["serviceSecondLevel"],
            "id_recurso": item["id"],
            "protocolo": item["protocol"],
            "status": item["status"],
            "data_recurso": item["createdDate"],
        }
        row.update(get_custom_fields(item["customFieldValues"]))
        data = data.append(row, ignore_index=True)

    data["timestamp_captura"] = timestamp

    # filtrando não nulos
    data = data.dropna()

    # converte datas
    data["data_recurso"] = (
        pd.to_datetime(data["data_recurso"])
        .dt.tz_localize(tz="America/Sao_Paulo")
        .map(lambda x: x.isoformat())
    )
    data["data_viagem"] = pd.to_datetime(data["data_viagem"]).dt.strftime("%Y-%m-%d")

    # remove caracteres de campo aberto que quebram o schema
    data["servico"] = data["servico"].str.replace("\xa0", " ").str.replace("\n", "")

    return {"data": data, "error": None}
