# -*- coding: utf-8 -*-
import os
import csv
import shutil
import pandas as pd
from prefect import task
from pipelines.utils.utils import log
from datetime import datetime, timedelta
from pipelines.rj_sms.utils import create_partitions, cloud_function_request
from pipelines.rj_sms.dump_api_prontuario_vitacare.constants import (
    constants as vitacare_constants,
)


@task
def get_patients(context):
    log("Getting data from cloud function")
    list_cnes = vitacare_constants.CNES.value
    if context == "scheduled":
        url = vitacare_constants.URL_PACIENTES_AGENDADOS.value
        data = datetime.today() + timedelta(days=3)
    else:
        url = vitacare_constants.URL_PACIENTES_ATENDIDOS.value
        data = datetime.today() - timedelta(days=1)
    data_formatada = data.strftime("%Y-%m-%d")
    df = pd.DataFrame()
    list_cnes_error = []
    list_cnes_empty = []
    for cnes in list_cnes:
        params = '{"cnes": "' + cnes + '", "date": "' + data_formatada + '"}'
        response = cloud_function_request.run(
            url=url, request_type="POST", body_params=params, env="prod"
        )
        if response.text.startswith("A solicitação não foi bem-sucedida"):
            list_cnes_error.append(cnes)
        else:
            try:
                df_temp = pd.read_json(response.text)
            except:
                log(f"Error cnes - {cnes}, Detail: {response.text}", level="error")
            if not df_temp.empty:
                df = pd.concat([df, df_temp], ignore_index=True)
            else:
                list_cnes_empty.append(cnes)
    log(f"List cnes error {list_cnes_error}", level="error")
    log(f"List cnes empty erro {list_cnes_empty}", level="error")
    return df


@task
def save_patients(dataframe, context):
    log("Saving data into the server")
    path = "pipelines/rj_sms/dump_api_prontuario_vitacare/data"
    try:
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
            os.mkdir(path)
        else:
            os.mkdir(path)
        if context == "scheduled":
            data_futura = datetime.today() + timedelta(days=3)
        else:
            data_futura = datetime.today() - timedelta(days=1)
        data_formatada = data_futura.strftime("%Y-%m-%d")
        filename = (
            f"pipelines/rj_sms/dump_api_prontuario_vitacare/data/{data_formatada}.csv"
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
            "pipelines/rj_sms/dump_api_prontuario_vitacare/data_partition"
        )
        shutil.rmtree(partition_directory, ignore_errors=True)
        create_partitions.run(
            "pipelines/rj_sms/dump_api_prontuario_vitacare/data", partition_directory
        )
        return True
    except:
        log("Error when trying to save files", level="error")
        return False
