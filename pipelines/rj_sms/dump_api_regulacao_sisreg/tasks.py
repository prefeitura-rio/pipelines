# -*- coding: utf-8 -*-
import os
import csv
import shutil
import time
import json
import requests
import pandas as pd
from prefect import task
from pipelines.utils.utils import log, get_vault_secret
from datetime import datetime, timedelta
from pipelines.rj_sms.tasks import create_partitions


@task
def get_patients(cnes):
    # Get Autentication
    url = "https://rest.smsrio.org/api/usuario/autenticar"

    # Retrieve the API key from Vault
    try:
        credential = get_vault_secret(secret_path="regulacao_sisreg")["data"]
    except Exception as e:
        log(f"Not able to retrieve Vault secret {e}", level="error")

    payload = json.dumps(credential)

    headers = {
        "Content-Type": "application/json",
        "Cookie": "PHPSESSID=b40302ab232addf99960f1d4ffa7073b",
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200:
        # Get token
        dados_json = json.loads(response.text)
        token = dados_json["dados"]
        data_futura = datetime.today() + timedelta(days=5)
        data_formatada = data_futura.strftime("%Y-%m-%d")
        url = f"https://web2.smsrio.org/ambulatorio/api/pacientesAgendados/{cnes}/{data_formatada}/"

        payload = ""
        headers = {"Authorization": "Bearer " + token}

        # Desired number of repetitions
        num_repeticoes = 5

        for _ in range(num_repeticoes):
            response = requests.get(url, headers=headers, data=payload)
            if response.status_code == 200:
                log("Solicitação bem-sucedida!")
                df = pd.read_json(response.text)
                if df.empty:
                    log("DataFrame is empty!")
                else:
                    return df
                break
            else:
                log(f"Falha na solicitação, código de status: {response.status_code}")
            # Aguarda 1 minuto antes da próxima solicitação
            time.sleep(10)

    return pd.DataFrame()


@task
def save_patients(dataframe):
    path = "pipelines/rj_sms/dump_api_regulacao_sisreg/data"
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
        os.mkdir(path)
    else:
        os.mkdir(path)
    data_futura = datetime.today() + timedelta(days=5)
    data_formatada = data_futura.strftime("%Y-%m-%d")
    filename = f"pipelines/rj_sms/dump_api_regulacao_sisreg/data/{data_formatada}.csv"
    dataframe.to_csv(
        filename,
        sep=";",
        quoting=csv.QUOTE_NONNUMERIC,
        quotechar='"',
        index=False,
        encoding="utf-8",
    )
    partition_directory = "pipelines/rj_sms/dump_api_regulacao_sisreg/data_partition"
    shutil.rmtree(partition_directory, ignore_errors=True)
    create_partitions.run(
        "pipelines/rj_sms/dump_api_regulacao_sisreg/data", partition_directory
    )
    return True
