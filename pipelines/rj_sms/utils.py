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


@task
def create_folders():
    try:

        if os.path.exists(os.path.expanduser("~") + "/data"):
            shutil.rmtree(os.path.expanduser("~") + "/data", ignore_errors=True)
        else:
            os.makedirs(os.path.expanduser("~") + "/data")

        if os.path.exists(os.path.expanduser("~") + "/partition_directory"):
            shutil.rmtree(os.path.expanduser("~") + "/partition_directory", ignore_errors=True)
        else:
            os.makedirs(os.path.expanduser("~") + "/partition_directory")
        
        log("Folders created", level="success")

    except:
        log("Folders could not be created", level="error")
  
@task
def download_api(
    url: str,
    destination_file_name: str,
    params=None,
    vault_path=None,
    vault_key=None,
    add_load_date_to_filename=False,
):
    auth_token = ""
    if vault_key is not None: 
        try:
            auth_token = get_vault_secret(secret_path=vault_path)["data"][vault_key]
            log("Vault secret retrieved", level="success")
        except:
            log("Not able to retrieve Vault secret", level="error")

    log("Downloading data from API", level="info")
    headers = {} if auth_token == "" else {"Authorization": f"Bearer {auth_token}"}
    params = {} if params is None else params
    try:
        response = requests.get(url, headers=headers, params=params)
    except:
        log(f"An error occurred: {Exception}", level="error")

    if response.status_code == 200:
        # The response contains the data from the API
        api_data = response.json()

        # Save the API data to a local file
        if add_load_date_to_filename:
            destination_file_path = f"{os.path.expanduser('~')}/data/{destination_file_name}_{str(date.today())}.json"
        else:
            destination_file_path = f"{os.path.expanduser('~')}/data/{destination_file_name}.json"
            

        # Save the API data to a local file
        with open(destination_file_path, "w") as file:
            file.write(str(api_data))

        log(f"API data downloaded to {destination_file_path}", level="success")

    else:
        log(f"Error: {response.status_code} - {response.reason}", level="error")

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
def from_json_to_csv(input_path, sep=";"):
    try:
        with open(input_path, "r") as file:
            json_data = file.read()
            data = eval(json_data)  # Convert JSON string to Python dictionary

            output_path = input_path.replace(".json", ".csv")
            # Assuming the JSON structure is a list of dictionaries
            df = pd.DataFrame(data, dtype="str")
            df.to_csv(output_path, index=False, sep=sep, encoding="utf-8")

            logger.success("JSON converted to CSV")
            return output_path

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None


@task
def create_partitions(data_path: str | Path, partition_directory: str | Path):
    
    # Check if partition directory exists, if not create it
    if not os.path.exists(partition_directory):
        os.makedirs(partition_directory)

    # Clean partition directory
    shutil.rmtree(partition_directory, ignore_errors=True)
    
    # Load data
    files = data_path.glob("*.csv")

    # Create partition directory
    for file_name in files:
        date_str = re.search(r"\d{4}-\d{2}-\d{2}", str(file_name)).group()
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        ano_particao = parsed_date.strftime("%Y")
        mes_particao = parsed_date.strftime("%m")
        data_particao = parsed_date.strftime("%Y-%m-%d")

        output_directory = (
            partition_directory
            / f"ano_particao={int(ano_particao)}"
            / f"mes_particao={int(mes_particao)}"
            / f"data_particao={data_particao}"
        )

        output_directory.mkdir(parents=True, exist_ok=True)

        # Copy file(s) to partition directory
        shutil.copy(file_name, output_directory)


