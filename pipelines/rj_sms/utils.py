# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log, get_vault_secret
import os
import requests
import pandas as pd
from datetime import date
from azure.storage.blob import BlobServiceClient


@task
def download_api(
    url: str, destination_file_name: str, vault_secret_path: str, vault_secret_key: str
):
    auth_token = get_vault_secret(secret_path=vault_secret_path)["data"][
        vault_secret_key
    ]

    headers = {"Authorization": f"Bearer {auth_token}"}

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            # The response contains the data from the API
            # api_data = response.json()

            # Save the API data to a local file
            destination_file_path = f"{os.path.expanduser('~')}/{destination_file_name}_{str(date.today())}.csv"

            df = pd.DataFrame(response.json(), dtype="str")
            df["_data_carga"] = date.today()
            df.to_csv(destination_file_path, index=False, sep=";", encoding="utf-8")

            # Save the API data to a local file

            # with open(destination_file_path, "w") as file:
            #    file.write(str(api_data))

            log("API data saved")

        else:
            log("Error:", response.status_code, response.text)

    except requests.exceptions.RequestException as e:
        log("An error occurred:", e)

    return destination_file_path


@task
def download_azure_blob(
    container_name: str,
    blob_path: str,
    destination_file_path: str,
    vault_path: str,
    vault_token,
):
    credential = get_vault_secret(secret_path=vault_path)["data"][vault_token]

    blob_service_client = BlobServiceClient(
        account_url="https://datalaketpcgen2.blob.core.windows.net/",
        credential=credential,
    )
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_path
    )

    with open(destination_file_path, "wb") as blob_file:
        blob_data = blob_client.download_blob()
        blob_data.readinto(blob_file)

    log(f"Blob downloaded to '{destination_file_path}'.")


@task
def clean_ascii(input_file_path):
    try:
        with open(input_file_path, "r", encoding="utf-8") as input_file:
            text = input_file.read()

            # Remove non-basic ASCII characters
            cleaned_text = "".join(c for c in text if ord(c) < 128)

            if ".json" in input_file_path:
                output_file_path = input_file_path.replace(".json", "_clean.json")
            elif ".csv" in input_file_path:
                output_file_path = input_file_path.replace(".csv", "_clean.csv")

            with open(output_file_path, "w", encoding="utf-8") as output_file:
                output_file.write(cleaned_text)

            log(f"Cleaning complete. Cleaned text saved at {output_file_path}")

            return output_file_path

    except Exception as e:
        log("An error occurred:", e)


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
def convert_to_parquet(input_file_path: str, schema: str):
    if ".json" in input_file_path:
        try:
            with open(input_file_path, "r") as file:
                json_data = file.read()
                data = eval(json_data)  # Convert JSON string to Python dictionary

                # Assuming the JSON structure is a list of dictionaries
                try:
                    df = pd.DataFrame(data, dtype=schema)
                    log("Dados carregados com schema")
                except:
                    df = pd.DataFrame(data)
                    log("Dados carregados sem schema")

            # TODO: adicionar coluna com a data da carga (_data_carga)

            destination_path = input_file_path.replace(".json", ".parquet")

        except Exception as e:
            log("An error occurred:", e)

    df.to_parquet(destination_path, index=False)
