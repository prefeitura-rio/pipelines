# -*- coding: utf-8 -*-
import csv
import shutil
import time
import json
import requests
import pandas as pd
from prefect import task
from pipelines.utils.utils import log
from datetime import datetime, timedelta
import basedosdados as bd

from utils import create_partitions


@task
def read_file():
    arquivo_excel = "pipelines/rj_sms/whatsapp/unidades_equipe_aps/data/unidades-equipes-aps-2023-10-27.csv"
    dataframe = pd.read_csv(arquivo_excel, sep=";")
    return dataframe


@task
def save_file(dataframe):
    data_futura = datetime.today() + timedelta(days=3)
    data_formatada = data_futura.strftime("%Y-%m-%d")
    filename = "pipelines/rj_sms/whatsapp/unidades_equipe_aps/data/unidades-equipes-aps-2023-10-27.csv"
    dataframe.to_csv(
        filename,
        sep=";",
        quoting=csv.QUOTE_NONNUMERIC,
        quotechar='"',
        index=False,
        encoding="utf-8",
    )
    partition_directory = "pipelines/rj_sms/whatsapp/unidades_equipe_aps/data_partition"
    shutil.rmtree(partition_directory, ignore_errors=True)
    create_partitions(
        "pipelines/rj_sms/whatsapp/unidades_equipe_aps/data", partition_directory
    )
    return True


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
