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
def get_patients(cnes, context):
    log("Getting data from cloud function")
    if context == 'scheduled':
        url = (
            "http://homologacao-devrj.pepvitacare.com:9003/health/schedule/nextappointments"
        )
        data = datetime.today() + timedelta(days=3)
    else:
        url = (
            "http://homologacao-devrj.pepvitacare.com:9003/health/schedule/nextappointments"
        )
        data = datetime.today() - timedelta(days=1)
    #data_formatada = data.strftime('%Y-%m-%d')
    data_formatada = '2023-10-24'
    df = pd.DataFrame()
    params = '{"cnes": "' + cnes + '", "date": "' + data_formatada + '"}'
    response = cloud_function_request.run(
        url=url, request_type="GET", body_params=params, env="staging"
    )
    df_temp = pd.read_json(response.text)
    if not df_temp.empty:
        df = pd.concat([df, df_temp], ignore_index=True)
    else:
        log('Error read data from cnes - ' + cnes, level='error' )
    return df

@task
def save_patients(dataframe, context):
    log("Saving data into the server")
    path = 'pipelines/rj_sms/dump_api_prontuario_vitacare/data'
    try:
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
            os.mkdir(path)
        else:
            os.mkdir(path)
        if context == 'scheduled':
            data_futura = datetime.today() + timedelta(days=3)
        else:
            data_futura = datetime.today() - timedelta(days=1)
        data_formatada = data_futura.strftime("%Y-%m-%d")
        filename = f"pipelines/rj_sms/dump_api_prontuario_vitacare/data/{data_formatada}.csv"
        dataframe.to_csv(
            filename,
            sep=";",
            quoting=csv.QUOTE_NONNUMERIC,
            quotechar='"',
            index=False,
            encoding="utf-8",
        )
        partition_directory = (
            "pipelines/rj_sms/dump_api_prontuario_vitacare/data_partition"
        )
        shutil.rmtree(partition_directory, ignore_errors=True)
        create_partitions.run(
            "pipelines/rj_sms/dump_api_prontuario_vitacare/data", partition_directory
        )
        return True
    except:
        log('Error when trying to save files', level='error')
        return False