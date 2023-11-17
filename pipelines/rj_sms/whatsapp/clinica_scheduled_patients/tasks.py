# -*- coding: utf-8 -*-
import csv
import shutil
import pandas as pd
from prefect import task
from pipelines.utils.utils import log
from utils import create_partitions, cloud_function_request
from datetime import datetime, timedelta
from pipelines.utils.utils import log, get_vault_secret

@task
def get_patients():
    url = 'http://saudedigital.pepvitacare.com:8081/health/schedule/nextappointments'
    params = "{\"cnes\": \"6688152\", \"date\": \"2023-11-13\"}"
    return cloud_function_request(url = url, request_type = 'POST', body_params = params, env = 'staging')

@task
def save_patients(dataframe):
    data_futura = datetime.today() + timedelta(days=3)
    data_formatada = data_futura.strftime('%Y-%m-%d')
    filename = f'pipelines/rj_sms/whatsapp/clinica_scheduled_patients/data/{data_formatada}.csv'
    dataframe.to_csv(filename, sep=';', quoting=csv.QUOTE_NONNUMERIC, quotechar='"', index=False, encoding='utf-8')
    partition_directory = 'pipelines/rj_sms/whatsapp/clinica_scheduled_patients/data_partition'
    shutil.rmtree(partition_directory, ignore_errors=True)
    create_partitions('pipelines/rj_sms/whatsapp/clinica_scheduled_patients/data', partition_directory)
    return True