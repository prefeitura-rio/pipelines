# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913
"""
General utilities for SMS pipelines.
"""

import os
import re
import shutil
import sys
from datetime import datetime, date
from pathlib import Path
import requests
import pandas as pd
import basedosdados as bd
from azure.storage.blob import BlobServiceClient
from prefect import task
from pipelines.utils.utils import log, get_vault_secret


@task
def create_folders():
    """
    Creates two directories, './data/raw' and './data/partition_directory', and returns their paths.

    Returns:
        dict: A dictionary with the paths of the created directories.
            - "data": "./data/raw"
            - "partition_directory": "./data/partition_directory"
    """
    try:
        path_raw_data = os.path.join(os.getcwd(), "data", "raw")
        path_partionared_data = os.path.join(os.getcwd(), "data", "partition_directory")

        if os.path.exists(path_raw_data):
            shutil.rmtree(path_raw_data, ignore_errors=False)
        os.makedirs(path_raw_data)

        if os.path.exists(path_partionared_data):
            shutil.rmtree(path_partionared_data, ignore_errors=False)
        os.makedirs(path_partionared_data)

        log("Folders created")
        return {
            "data": path_raw_data,
            "partition_directory": path_partionared_data,
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
    """
    Downloads data from an API and saves it to a local file.

    Args:
        url (str): The URL of the API to download data from.
        file_folder (str): The folder where the downloaded file will be saved.
        file_name (str): The name of the downloaded file.
        params (dict, optional): Additional parameters to include in the API request.
        vault_path (str, optional): The path in Vault where the authentication token is stored.
        vault_key (str, optional): The key in Vault where the authentication token is stored.
        add_load_date_to_filename (bool, optional): Whether to add the current date to the filename.
        load_date (str, optional): The specific date to add to the filename.

    Returns:
        str: The path of the downloaded file.
    """
    # Retrieve the API key from Vault
    auth_token = ""
    if vault_key is not None:
        try:
            auth_token = get_vault_secret(secret_path=vault_path)["data"][vault_key]
            log("Vault secret retrieved")
        except Exception as e:
            log(f"Not able to retrieve Vault secret {e}", level="error")

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

        with open(destination_file_path, "w", encoding="utf-8") as file:
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
    """
    Downloads a blob from Azure Blob Storage and saves it to the specified destination file path.

    Args:
        container_name (str): The name of the container in the Azure Blob Storage account.
        blob_path (str): The path to the blob in the container.
        destination_file_path (str): The path to save the downloaded blob file.
        vault_path (str): The path to the secret in Vault.
        vault_token (str): The token to access the secret in Vault.

    Returns:
        None

    Raises:
        Exception: If there is an error retrieving the Vault secret.

    Example Usage:
        download_azure_blob(
            container_name="my-container",
            blob_path="path/to/blob",
            destination_file_path="path/to/destination/file",
            vault_path="path/to/vault/secret",
            vault_token="token"
        )
    """
    try:
        credential = get_vault_secret(secret_path=vault_path)["data"][vault_token]
        log("Vault secret retrieved")
    except Exception as e:
        log(f"Not able to retrieve Vault secret {e}", level="error")

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
    """
    Clean ASCII Function

    This function removes any non-basic ASCII characters from the text and saves the cleaned text
    to a new file with a modified file name.
    Args:
        input_file_path (str): The path of the input file.

    Returns:
        str: The path of the output file containing the cleaned text.
    """

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
        log(f"An error occurred: {e}", level="error")


@task
def set_destination_file_path(file):
    """
    Returns the destination file path based on the given file name and default path (~).

    Args:
        file (str): The name of the file.

    Returns:
        str: The constructed destination file path.
    """
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
    """
    Adds a new column '_data_carga' to a CSV file located at input_path with the current date
    or a specified load date.

    Args:
        input_path (str): The path to the input CSV file.
        sep (str, optional): The delimiter used in the CSV file. Defaults to ";".
        load_date (str, optional): The load date to be used in the '_data_carga' column. If None,
        the current date is used. Defaults to None.

    Returns:
        str: The path to the input CSV file.
    """
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
    """
    Converts a JSON file to a CSV file.

    Args:
        input_path (str): The path to the input JSON file.
        sep (str, optional): The separator to use in the output CSV file. Defaults to ";".

    Returns:
        str: The path to the output CSV file, or None if an error occurred.
    """
    try:
        with open(input_path, "r", encoding="utf-8") as file:
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
    """
    Creates partitions for each file in the given data path and saves them in the
    partition directory.

    Args:
        data_path (str): The path to the data directory.
        partition_directory (str): The path to the partition directory.

    Raises:
        FileExistsError: If the partition directory already exists.

    Returns:
        None
    """
    data_path = Path(data_path)
    # Load data
    files = data_path.glob("*.csv")
    #
    # Create partition directories for each file
    for file_name in files:
        date_str = re.search(r"\d{4}-\d{2}-\d{2}", str(file_name)).group()
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        ano_particao = parsed_date.strftime("%Y")
        mes_particao = parsed_date.strftime("%m")
        data_particao = parsed_date.strftime("%Y-%m-%d")

        output_directory = f"{partition_directory}/ano_particao={int(ano_particao)}/mes_particao={int(mes_particao)}/data_particao={data_particao}"  # noqa: E501

        # Create partition directory
        os.makedirs(output_directory, exist_ok=False)

        # Copy file(s) to partition directory
        shutil.copy(file_name, output_directory)
        log("Partitions created successfully")


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
    """
    Uploads data from a file to a BigQuery table in a specified dataset.

    Args:
        input_path (str): The path to the file containing the data to be uploaded.
        dataset_id (str): The ID of the dataset where the table is located.
        table_id (str): The ID of the table where the data will be uploaded.
        if_exists (str, optional): Specifies what to do if the table already exists.
            Defaults to "replace".
        csv_delimiter (str, optional): The delimiter used in the CSV file. Defaults to ";".
        if_storage_data_exists (str, optional): Specifies what to do if the storage data
            already exists. Defaults to "replace".
        biglake_table (bool, optional): Specifies whether the table is a BigLake table.
            Defaults to True.
    """
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
