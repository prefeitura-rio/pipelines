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
from ftplib import FTP
import zipfile
import requests
import pytz
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

        folders = {
            "raw": path_raw_data,
            "partition_directory": path_partionared_data,
        }

        log(f"Folders created: {folders}")
        return folders

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
    file_folder: str,
    file_name: str,
    vault_path: str,
    vault_key: str,
    add_load_date_to_filename=False,
    load_date=None,
):
    """
    Downloads data from Azure Blob Storag and saves it to a local file.

    Args:
        container_name (str): The name of the container in the Azure Blob Storage account.
        blob_path (str): The path to the blob in the container.
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
    try:
        credential = get_vault_secret(secret_path=vault_path)["data"][vault_key]
        log("Vault secret retrieved")
    except Exception as e:
        log(f"Not able to retrieve Vault secret {e}", level="error")

    # Download data from Blob Storage
    log(f"Downloading data from Azure Blob Storage: {blob_path}")
    blob_service_client = BlobServiceClient(
        account_url="https://datalaketpcgen2.blob.core.windows.net/",
        credential=credential,
    )
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_path
    )

    # Save the API data to a local file
    if add_load_date_to_filename:
        if load_date is None:
            destination_file_path = f"{file_folder}/{file_name}_{str(date.today())}.csv"
        else:
            destination_file_path = f"{file_folder}/{file_name}_{load_date}.csv"
    else:
        destination_file_path = f"{file_folder}/{file_name}.csv"

    with open(destination_file_path, "wb") as file:
        blob_data = blob_client.download_blob()
        blob_data.readinto(file)

    log(f"Blob downloaded to '{destination_file_path}")

    return destination_file_path


@task
def download_ftp(
    host: str,
    user: str,
    password: str,
    directory: str,
    file_name: str,
    output_path: str,
):
    """
    Downloads a file from an FTP server and saves it to the specified output path.

    Args:
        host (str): The FTP server hostname.
        user (str): The FTP server username.
        password (str): The FTP server password.
        directory (str): The directory on the FTP server where the file is located.
        file_name (str): The name of the file to download.
        output_path (str): The local path where the downloaded file should be saved.

    Returns:
        str: The local path where the downloaded file was saved.
    """

    file_path = f"{directory}/{file_name}"
    output_path = output_path + "/" + file_name
    log(output_path)
    # Connect to the FTP server
    ftp = FTP(host)
    ftp.login(user, password)

    # Get the size of the file
    ftp.voidcmd("TYPE I")
    total_size = ftp.size(file_path)

    # Create a callback function to be called when each block is read
    def callback(block):
        nonlocal downloaded_size
        downloaded_size += len(block)
        percent_complete = (downloaded_size / total_size) * 100
        if percent_complete // 5 > (downloaded_size - len(block)) // (total_size / 20):
            log(f"Download is {percent_complete:.0f}% complete")
        f.write(block)

    # Initialize the downloaded size
    downloaded_size = 0

    # Download the file
    with open(output_path, "wb") as f:
        ftp.retrbinary(f"RETR {file_path}", callback)

    # Close the connection
    ftp.quit()

    return output_path


@task
def list_files_ftp(host, user, password, directory):
    """
    Lists all files in a given directory on a remote FTP server.

    Args:
        host (str): The hostname or IP address of the FTP server.
        user (str): The username to use for authentication.
        password (str): The password to use for authentication.
        directory (str): The directory to list files from.

    Returns:
        list: A list of filenames in the specified directory.
    """
    ftp = FTP(host)
    ftp.login(user, password)
    ftp.cwd(directory)

    files = ftp.nlst()

    ftp.quit()

    return files


@task
def unzip_file(file_path: str, output_path: str):
    """
    Unzips a file to the specified output path.

    Args:
        file_path (str): The path to the file to be unzipped.
        output_path (str): The path to the output directory.

    Returns:
        str: The path to the unzipped file.
    """
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(output_path)

    log(f"File unzipped to {output_path}")

    return output_path


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
    tz = pytz.timezone("Brazil/East")
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    df = pd.read_csv(input_path, sep=sep, keep_default_na=False, dtype="str")

    if load_date is None:
        df["_data_carga"] = now
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
        if not os.path.exists(output_directory):
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
    dump_mode: str = "append",
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
    table_staging = f"{tb.table_full_name['staging']}"
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    storage_path_link = (
        f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
        f"/staging/{dataset_id}/{table_id}"
    )

    try:
        table_exists = tb.table_exists(mode="staging")

        if not table_exists:
            log(f"CREATING TABLE: {dataset_id}.{table_id}")
            tb.create(
                path=input_path,
                csv_delimiter=csv_delimiter,
                if_storage_data_exists=if_storage_data_exists,
                biglake_table=biglake_table,
            )
        else:
            if dump_mode == "append":
                log(
                    f"TABLE ALREADY EXISTS APPENDING DATA TO STORAGE: {dataset_id}.{table_id}"
                )

                tb.append(filepath=input_path, if_exists=if_exists)
            elif dump_mode == "overwrite":
                log(
                    "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                    f"{storage_path}\n"
                    f"{storage_path_link}"
                )  # pylint: disable=C0301
                st.delete_table(
                    mode="staging", bucket_name=st.bucket_name, not_found_ok=True
                )
                log(
                    "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                    f"{storage_path}\n"
                    f"{storage_path_link}"
                )  # pylint: disable=C0301
                tb.delete(mode="all")
                log(
                    "MODE OVERWRITE: Sucessfully DELETED TABLE:\n" f"{table_staging}\n"
                )  # pylint: disable=C0301

                tb.create(
                    path=input_path,
                    csv_delimiter=csv_delimiter,
                    if_storage_data_exists=if_storage_data_exists,
                    biglake_table=biglake_table,
                )
        log("Data uploaded to BigQuery")

    except Exception as e:
        log(f"An error occurred: {e}", level="error")