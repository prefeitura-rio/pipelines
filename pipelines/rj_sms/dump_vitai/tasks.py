# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log, get_vault_secret
from pipelines.rj_sms.utils import clean_ascii
import os
import pandas as pd
import requests
from datetime import date

@task
def download_api(url: str, file_name):

    auth_token = get_vault_secret(secret_path="estoque_vitai")["data"]["token"]

    headers = {"Authorization": f"Bearer {auth_token}"}

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            # The response contains the data from the API
            api_data = response.json()

            # Save the API data to a local file
            destination_file_path = f"{os.path.expanduser('~')}/{file_name}_{str(date.today())}.json"

            with open(destination_file_path, 'w') as file:
                file.write(str(api_data))

            log("API data saved to 'api_data.json'")

        else:
            log("Error:", response.status_code, response.text)

    except requests.exceptions.RequestException as e:
        log("An error occurred:", e)

    return destination_file_path

@task
def set_destination_file_path(file):
    return (
        os.path.expanduser("~")
        + "/"
        + file[: file.find(".")]
        + "_"
        + str(date.today())
        + file[file.find(".") :]
    )


@task
def clean_json(filepath: str):
    destination_path = f"{os.path.expanduser('~')}/estoque_limpo.json"
    clean_ascii(filepath, destination_path)

    return destination_path

@task
def fix_payload_vitai(filepath: str):
    df = pd.read_csv(filepath, sep=";", keep_default_na=False, dtype={"cnes": "str"})

    # remove caracteres que confundem o parser
    df["descricao"] = df.descricao.apply(lambda x: x.replace('"', ""))
    df["descricao"] = df.descricao.apply(lambda x: x.replace(",", ""))

    df.to_csv(filepath, index=False, sep=",", encoding="utf-8", quoting=0, decimal=".")
