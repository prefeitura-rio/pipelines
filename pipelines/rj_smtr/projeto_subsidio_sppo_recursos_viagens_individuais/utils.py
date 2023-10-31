# -*- coding: utf-8 -*-
import time
import json
import math
from typing import Dict
import pandas as pd
import requests

# EMD Imports #

from pipelines.utils.utils import log, get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.constants import constants


def create_api_url_recursos(date_range_end, skip=0, top=1000) -> Dict:
    """
    Returns movidesk URL to get the requests over date range.
    """
    url = constants.SUBSIDIO_SPPO_RECURSO_API_BASE_URL.value
    token = get_vault_secret(constants.SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH.value)[
        "data"
    ]["token"]

    end = pd.to_datetime(date_range_end, utc=True).strftime("%Y-%m-%dT%H:%M:%S.%MZ")

    dates = f"createdDate le {end}"
    service = "serviceFull eq 'SPPO'"

    params = {
        "select": "id," "protocol," "createdDate",
        "filter": f"{dates} and serviceFull/any(serviceFull: {service})",
        "expand": "customFieldValues,"
        "customFieldValues($expand=items),"
        "actions($select=id,description)",
        "top": top,
        "skip": skip,
    }

    log(f"URL Parameters: {params}")

    url += f"token={token}"

    for param, value in params.items():
        url += f"&${param}={value}"

    # log(f'url: {url}')

    return url


def request_data(url: str) -> Dict:
    """
    Return response json and error.
    """
    retries = 0

    # Get data from API
    while retries <= constants.MAX_RETRIES.value:
        try:
            response = requests.get(url, timeout=constants.MAX_TIMEOUT_SECONDS.value)

        except Exception as error:
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")
            return {"data": None, "error": True}

        # Check data results
        if response.ok:  # status code is less than 400
            data = response.json()
            return {"data": data, "error": False}

        else:
            print(f"Falha no requests. Tentativa: {retries}.")
            retries += 1
            if retries > constants.MAX_RETRIES.value:
                return {"data": None, "error": True}
            time.sleep(30)


def trata_motivo(valor: dict):
    if valor == "Não respeitou o limite da conformidade da distância da viagem.":
        return "Não respeitou o limite da conformidade da distância da viagem."
    elif (
        valor
        == "Informação incompleta ou incorreta (Art. 5o § 2o Res. SMTR 3534 / 2022)"
    ):
        return "Informação incompleta ou incorreta (Art. 5º § 2º Res. SMTR 3534 / 2022)"
    elif (
        valor
        == "Não respeitou o limite da conformidade da quantidade de \
        transmissões dentro do itinerário. (Art 2o Res. SMTR 3534 / 2022)"
    ):
        return "Não respeitou o limite da conformidade da quantidade de \
            transmissões dentro do itinerário. (Art 2º Res. SMTR 3534 / 2022)"
    elif (
        valor
        == "Não houve comunicação de GPS no período informado para o veículo. \
            (Art 1o Res. SMTR 3534 / 2022)"
    ):
        return "Não houve comunicação de GPS no período informado para o veículo. \
            (Art 1º Res. SMTR 3534 / 2022)"
    elif (
        valor
        == "Intempestivo. Recurso de viagem fora do prazo. \
              (Art. 5o Res. SMTR 3534 / 2022)"
    ):
        return "Intempestivo. Recurso de viagem fora do prazo. \
              (Art. 5º Res. SMTR 3534 / 2022)"
    elif (
        valor
        == "Não respeitou o limite da conformidade da qualidade do GPS.\
              (Art 2o Res. SMTR 3534 / 2022)"
    ):
        return "Não respeitou o limite da conformidade da qualidade do GPS.\
              (Art 2º Res. SMTR 3534 / 2022)"
    elif valor == "Viagem já paga.":
        return "Viagem já paga."
    elif (
        valor
        == "Viagem identificada considerando os sinais de GPS com o \
            serviço informado pelo recurso."
    ):
        return "Viagem identificada considerando os sinais de GPS com o \
            serviço informado pelo recurso."
    else:
        return valor


def analisar_ja_julgados(row):
    """
    Analisa se o recurso já foi julgado anteriormente, se tiver sido, \
        retira o julgamento anterior.
    """
    # Encontrar o índice do dicionário desejado
    indice_para_remover = None
    for indice, dicionario in enumerate(row["customFieldValues"]):
        if (
            dicionario["customFieldId"] == 111904
            or dicionario["customFieldId"] == 111900
        ):
            indice_para_remover = indice
            break

    # Remover o dicionário, se encontrado
    if indice_para_remover is not None:
        del row["customFieldValues"][indice_para_remover]

    # Resultado
    return row


def analisar_linha(row: dict) -> Dict:
    """
    Treat the row to return the values of the json
    """

    julgamento = {
        "items": [
            {
                "personId": None,
                "clientId": None,
                "team": None,
                "customFieldItem": row["julgamento"],
                "storageFileGuid": "",
                "fileName": None,
            }
        ],
        "customFieldId": 111865,
        "customFieldRuleId": 55533,
        "line": 1,
        "value": None,
    }

    row["customFieldValues"].append(julgamento)

    if row["julgamento"] == "Indeferido":
        id_julgamento = 111904
        rule_id = 55547
    else:
        id_julgamento = 111900
        rule_id = 55546

    motivo = {
        "items": [
            {
                "personId": None,
                "clientId": None,
                "team": None,
                "customFieldItem": row["motivo"],
                "storageFileGuid": "",
                "fileName": None,
            }
        ],
        "customFieldId": id_julgamento,
        "customFieldRuleId": rule_id,
        "line": 1,
        "value": None,
    }

    if (
        row["observacao"] == "nan"
        or row["observacao"] == ""
        or (isinstance(row["observacao"], float) and math.isnan(row["observacao"]))
    ):
        row["observacao"] = None

    observacao = {
        "customFieldId": 125615,
        "customFieldRuleId": rule_id,
        "line": 1,
        "value": row["observacao"],
        "items": [],
    }

    # Não da para adicionar no customFieldValues,
    # pois não da para fazer o PATCH do motivo junto com o do Julgamento
    row["json_motivo"] = motivo
    row["json_observacao"] = observacao

    return row


def patch_data(data: dict) -> Dict:
    """
    Post treat data to customField
    """
    count = 18932
    data["Patch_status"] = ""
    data["Patch_date"] = ""

    url = constants.SUBSIDIO_SPPO_RECURSO_API_BASE_URL.value
    token = get_vault_secret(constants.SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH.value)[
        "data"
    ]["token"]

    url = f"{url}token={token}"
    headers = {"Content-Type": "application/json"}

    for i, row in data.iterrows():
        retries = 0
        dual_patch = False

        while retries <= constants.MAX_RETRIES.value:
            if dual_patch:
                break

            id = row["protocolo"]
            body = {"customFieldValues": eval(row["customFieldValues"])}

            try:
                response = requests.patch(
                    f"{url}&id={id}",
                    data=json.dumps(body),
                    headers=headers,
                    timeout=constants.TIMEOUT.value,
                )
                response.raise_for_status()
                # Levanta uma exceção se a resposta não for bem-sucedida
            except requests.RequestException as exp:
                log(f"Erro: {exp} ao atualizar o ticket: {id}")
                retries += 1
                sleep_time = constants.BACKOFF_FACTOR.value * (2**retries)
                log(f"Tentando novamente em {sleep_time} segundos...")
                time.sleep(sleep_time)
                continue

            while retries <= constants.MAX_RETRIES.value:
                body["customFieldValues"].append(eval(row["json_motivo"]))
                body["customFieldValues"].append(eval(row["json_observacao"]))
                motive_and_observation_body = {
                    "customFieldValues": body["customFieldValues"],
                    "status": "Resolvido",
                    "baseStatus": "Resolved",
                    "justification": None,
                }

                try:
                    response = requests.patch(
                        f"{url}&id={id}",
                        data=json.dumps(motive_and_observation_body),
                        headers=headers,
                        timeout=constants.TIMEOUT.value,
                    )
                    response.raise_for_status()
                except requests.RequestException as exp:
                    data.loc[i, "Patch_status"] = str(exp)
                    data.loc[i, "Patch_date"] = time.time()
                    log(f"Erro: {exp} ao atualizar o ticket: {id}")
                    retries += 1
                    sleep_time = constants.BACKOFF_FACTOR.value * (2**retries)
                    log(f"Tentando novamente em {sleep_time} segundos...")
                    time.sleep(sleep_time)
                    continue

                data.loc[i, "Patch_status"] = response.status_code
                data.loc[i, "Patch_date"] = time.time()
                log(
                    f"PATCH de número {count} feito com sucesso. Ticket: {id} atualizado."
                )
                count += 1
                dual_patch = True
                break

        if retries >= constants.MAX_RETRIES.value:
            log(
                f"Ocorreram {constants.MAX_RETRIES.value} erros consecutivos \
                    ao atualizar o ticket: {id}. Encerrando o programa."
            )
            data.to_excel("comprovante_patch_incompleto_recursos_movidesk.xlsx")
            return data

    data.to_excel("comprovante_patch_recursos_movidesk.xlsx")
    return data
