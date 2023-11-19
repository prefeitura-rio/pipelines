# -*- coding: utf-8 -*-
import os
import csv
import shutil
import pandas as pd
from prefect import task
from pipelines.utils.utils import log
from pipelines.rj_sms.utils import create_partitions, cloud_function_request
from datetime import datetime, timedelta


@task
def get_patients():
    url = (
        "http://homologacao-devrj.pepvitacare.com:9003/health/schedule/nextappointments"
    )
    params = '{"cnes": "6688152", "date": "2023-10-27"}'
    response = cloud_function_request.run(
        url=url, request_type="GET", body_params=params, env="staging"
    )
    return pd.read_json(response.text)


@task
def save_patients(dataframe):
    path = 'pipelines/rj_sms/whatsapp/clinica_scheduled_patients/data'
    if not os.path.exists(path):
        os.mkdir(path)
    data_futura = datetime.today() + timedelta(days=3)
    data_formatada = data_futura.strftime("%Y-%m-%d")
    filename = f"pipelines/rj_sms/whatsapp/clinica_scheduled_patients/data/{data_formatada}.csv"
    dataframe.to_csv(
        filename,
        sep=";",
        quoting=csv.QUOTE_NONNUMERIC,
        quotechar='"',
        index=False,
        encoding="utf-8",
    )
    partition_directory = (
        "pipelines/rj_sms/whatsapp/clinica_scheduled_patients/data_partition"
    )
    shutil.rmtree(partition_directory, ignore_errors=True)
    create_partitions.run(
        "pipelines/rj_sms/whatsapp/clinica_scheduled_patients/data", partition_directory
    )
    return True
