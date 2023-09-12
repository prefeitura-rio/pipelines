# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log
from pipelines.rj_sms.utils import download_api
import pandas as pd
from datetime import date
import basedosdados as bd
from loguru import logger
import requests
from pipelines.utils.utils import log


@task
def build_params():
    params = {"date": str(date.today())}
    logger.success(f"Params built: {params}")
    return params


@task
def conform_csv_to_gcp(input_path: str):
    df = pd.read_csv(input_path, sep=";", keep_default_na=False, dtype="str")

    # remove caracteres que confundem o parser
    # df["descricao"] = df.descricao.apply(lambda x: x.replace('"', ""))
    # df["descricao"] = df.descricao.apply(lambda x: x.replace(",", ""))

    # add data da carga
    df["_data_carga"] = date.today()

    df.to_csv(input_path, index=False, sep="Æ", encoding="utf-8")
    logger.success("CSV now conform")

    return input_path


@task
def upload_to_datalake(input_path, dataset_id, table_id):
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    tb.create(
        path=input_path,
        csv_delimiter="Æ",
        csv_skip_leading_rows=1,
        csv_allow_jagged_rows=False,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="replace",
        biglake_table=True,
    )


@task
def get_public_ip():
    try:
        # Use a public IP address API to fetch your IP address
        response = requests.get("https://api64.ipify.org?format=json")

        if response.status_code == 200:
            data = response.json()
            log(f"IP: {data['ip']}")
        else:
            log(f"Failed to retrieve IP address. Status Code: {response.status_code}")
    except Exception as e:
        log(f"An error occurred: {str(e)}")

    return None
