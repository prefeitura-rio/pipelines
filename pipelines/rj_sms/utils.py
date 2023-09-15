# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log, get_vault_secret
import os
import requests
import pandas as pd
from datetime import date
from azure.storage.blob import BlobServiceClient
import re
import shutil
from datetime import datetime
from pathlib import Path
import basedosdados as bd
import sys


@task
def create_folders():
    try:
        if os.path.exists("./data/raw"):
            shutil.rmtree("./data/raw", ignore_errors=False)
        os.makedirs("./data/raw")

        if os.path.exists("./data/partition_directory"):
            shutil.rmtree("./data/partition_directory", ignore_errors=False)
        os.makedirs("./data/partition_directory")

        log("Folders created")
        return {
            "data": "./data/raw",
            "partition_directory": "./data/partition_directory",
        }

    except Exception as e:
        sys.exit(f"Failed to create folders: {e}")


@task
def download_from_api(
    url: str,
    file_folder: str,
    file_name: str,
    params=None,
    vault_path=None,
    vault_key=None,
    add_load_date_to_filename=False,
    load_date=None,
):
    log(url)
    log(load_date)
    # Retrieve the API key from Vault
    auth_token = ""
    if vault_key is not None:
        try:
            auth_token = get_vault_secret(secret_path=vault_path)["data"][vault_key]
            log("Vault secret retrieved")
        except:
            log("Not able to retrieve Vault secret", level="error")

    # Download data from API
    log("Downloading data from API")
    headers = {} if auth_token == "" else {"Authorization": f"Bearer {auth_token}"}
    params = {} if params is None else params
    try:
        response = requests.get(url, headers=headers, params=params)
    except Exception as e:
        log(f"An error occurred: {e}", level="error")

    if response.status_code == 200:
        api_data = response.json()

        # Save the API data to a local file
        if add_load_date_to_filename:
            if load_date is None:
                destination_file_path = (
                    f"{file_folder}/{file_name}_{str(date.today())}.json"
                )
            else:
                destination_file_path = f"{file_folder}/{file_name}_{load_date}.json"
        else:
            destination_file_path = f"{file_folder}/{file_name}.json"

        with open(destination_file_path, "w") as file:
            file.write(str(api_data))

        log(f"API data downloaded to {destination_file_path}")

    else:
        log(
            f"API call failed. Error: {response.status_code} - {response.reason}",
            level="error",
        )

    return destination_file_path


@task
def download_azure_blob(
    container_name: str,
    blob_path: str,
    destination_file_path: str,
    vault_path: str,
    vault_token,
):
    try:
        credential = get_vault_secret(secret_path=vault_path)["data"][vault_token]
        logger.success("Vault secret retrieved")
    except:
        logger.error("Not able to retrieve Vault secret")

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

    logger.success(f"Blob downloaded to '{destination_file_path}'.")


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
def add_load_date_column(input_path: str, sep=";", load_date=None):
    df = pd.read_csv(input_path, sep=sep, keep_default_na=False, dtype="str")

    if load_date is None:
        df["_data_carga"] = str(date.today())
    else:
        df["_data_carga"] = load_date

    df.to_csv(input_path, index=False, sep=sep, encoding="utf-8")
    log(f"Column added to {input_path}")

    return input_path


@task
def from_json_to_csv(input_path, sep=";"):
    try:
        with open(input_path, "r") as file:
            json_data = file.read()
            data = eval(json_data)  # Convert JSON string to Python dictionary

            output_path = input_path.replace(".json", ".csv")
            # Assuming the JSON structure is a list of dictionaries
            df = pd.DataFrame(data, dtype="str")
            df.to_csv(output_path, index=False, sep=sep, encoding="utf-8")

            log("JSON converted to CSV")
            return output_path

    except Exception as e:
        log(f"An error occurred: {e}", level="error")
        return None


@task
def create_partitions(data_path: str, partition_directory: str):
    data_path = Path(data_path)
    partition_directory = partition_directory
    ## Load data
    files = data_path.glob("*.csv")
    #
    ## Create partition directories for each file
    for file_name in files:
        date_str = re.search(r"\d{4}-\d{2}-\d{2}", str(file_name)).group()
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        ano_particao = parsed_date.strftime("%Y")
        mes_particao = parsed_date.strftime("%m")
        data_particao = parsed_date.strftime("%Y-%m-%d")

        output_directory = f"{partition_directory}/ano_particao={int(ano_particao)}/mes_particao={int(mes_particao)}/data_particao={data_particao}"

        # Create partition directory
        os.makedirs(output_directory, exist_ok=False)

        # Copy file(s) to partition directory
        shutil.copy(file_name, output_directory)
        log("Partitions created successfully")


#
#
@task
def upload_to_datalake(
    input_path: str,
    dataset_id: str,
    table_id: str,
    if_exists: str = "replace",
    csv_delimiter: str = ";",
    if_storage_data_exists: str = "replace",
    biglake_table: bool = True,
):
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    table_exists = tb.table_exists(mode="staging")

    try:
        if not table_exists:
            log(f"CREATING TABLE: {dataset_id}.{table_id}")
            tb.create(
                path=input_path,
                csv_delimiter=csv_delimiter,
                if_storage_data_exists=if_storage_data_exists,
                biglake_table=biglake_table,
            )
        else:
            log(
                f"TABLE ALREADY EXISTS APPENDING DATA TO STORAGE: {dataset_id}.{table_id}"
            )

            tb.append(filepath=input_path, if_exists=if_exists)
        log("Data uploaded to BigQuery")
    except Exception as e:
        log(f"An error occurred: {e}", level="error")
