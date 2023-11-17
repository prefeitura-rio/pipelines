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
def get_patients():
    # Get Autentication
    url = "https://rest.smsrio.org/api/usuario/autenticar"

    payload = json.dumps(
        {"cpf": "SisregAmb", "senha": "77HtOzVJ6^#d", "cnes": "5462886"}
    )

    headers = {
        "Content-Type": "application/json",
        "Cookie": "PHPSESSID=b40302ab232addf99960f1d4ffa7073b",
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200:
        # Get token
        dados_json = json.loads(response.text)
        token = dados_json["dados"]
        data_futura = datetime.today() + timedelta(days=3)
        data_formatada = data_futura.strftime("%Y-%m-%d")
        # Config Parameter
        list_cnes = ["6688152"]
        for cnes in list_cnes:
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
                    log(
                        f"Falha na solicitação, código de status: {response.status_code}"
                    )

                # Aguarda 1 minuto antes da próxima solicitação
                time.sleep(10)

    return pd.DataFrame()


@task
def save_patients(dataframe):
    data_futura = datetime.today() + timedelta(days=3)
    data_formatada = data_futura.strftime("%Y-%m-%d")
    filename = (
        f"pipelines/rj_sms/whatsapp/sisreg_scheduled_patients/data/{data_formatada}.csv"
    )
    dataframe.to_csv(
        filename,
        sep=";",
        quoting=csv.QUOTE_NONNUMERIC,
        quotechar='"',
        index=False,
        encoding="utf-8",
    )
    partition_directory = (
        "pipelines/rj_sms/whatsapp/sisreg_scheduled_patients/data_partition"
    )
    shutil.rmtree(partition_directory, ignore_errors=True)
    create_partitions(
        "pipelines/rj_sms/whatsapp/sisreg_scheduled_patients/data", partition_directory
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
