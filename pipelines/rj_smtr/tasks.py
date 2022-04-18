# -*- coding: utf-8 -*-
"""
Tasks for rj_smtr
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################
from basedosdados import Table, Storage
import json
import os
import pandas as pd
from pathlib import Path
import pendulum
from prefect import task
import requests

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import create_or_append_table
from pipelines.utils.utils import log


@task
def create_current_date_hour_partition():
    timezone = constants.TIMEZONE.value

    capture_time = pendulum.now(timezone)
    date = capture_time.strftime("%Y-%m-%d")
    hour = capture_time.strftime("%H")
    filename = capture_time.strftime("%Y-%m-%d-%H-%M-%S")

    partitions = f"data={date}/hora={hour}"

    return {
        "filename": filename,
        "partitions": partitions,
    }


@task
def get_file_path_and_partitions(dataset_id, table_id, filename, partitions):

    # If not specific table_id, use resource one
    # if not table_id:
    #     table_id = context.resources.basedosdados_config["table_id"]
    # dataset_id = context.resources.basedosdados_config["dataset_id"]

    # Get data folder from environment variable
    data_folder = os.getenv("DATA_FOLDER", "data")

    file_path = f"{os.getcwd()}/{data_folder}/{{mode}}/{dataset_id}/{table_id}/{partitions}/{filename}.{{filetype}}"
    log(f"creating file path {file_path}")

    return file_path


@task
def save_raw_local(data, file_path, mode="raw"):

    _file_path = file_path.format(mode=mode, filetype="json")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    json.dump(data.json(), Path(_file_path).open("w", encoding="utf-8"))

    return _file_path


@task
def save_treated_local(dataframe, file_path, mode="staging"):

    _file_path = file_path.format(mode=mode, filetype="csv")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(_file_path, index=False)

    return _file_path


@task
def get_raw(url, headers=None):

    data = None
    error = None
    timestamp = pendulum.now(constants.TIMEZONE.value)
    try:
        data = requests.get(
            url, headers=headers, timeout=constants.MAX_TIMEOUT_SECONDS.value
        )
    except requests.exceptions.ReadTimeout as err:
        error = err
    except Exception as err:
        error = f"Unknown exception while trying to fetch data from {url}: {err}"

    if data is None:
        if error is None:
            error = "Data from API is none!"

    if error:
        return {"data": data, "timestamp": timestamp.isoformat(), "error": error}
    elif data.ok:
        return {
            "data": data,
            "error": error,
            "timestamp": timestamp.isoformat(),
        }
    else:
        error = f"Requests failed with error {data.status_code}"
        return {"error": error, "timestamp": timestamp.isoformat(), "data": data}


@task
def bq_upload(dataset_id, table_id, filepath, raw_filepath=None, partitions=None):
    log(
        f"""
    Received inputs:
    raw_filepath = {raw_filepath}, type = {type(raw_filepath)}
    treated_filepath = {filepath}, type = {type(filepath)}
    dataset_id = {dataset_id}, type = {type(dataset_id)}
    table_id = {table_id}, type = {type(table_id)}
    partitions = {partitions}, type = {type(partitions)}
    """
    )
    # Upload raw to staging
    if raw_filepath:
        st_obj = Storage(table_id=table_id, dataset_id=dataset_id)
        log(
            f"Uploading raw file: {raw_filepath} to bucket {st_obj.bucket_name} at {st_obj.bucket_name}/{dataset_id}/{table_id}"
        )
        st_obj.upload(
            path=raw_filepath, partitions=partitions, mode="raw", if_exists="replace"
        )

    # creates and publish table if it does not exist, append to it otherwise
    if partitions:
        # If table is partitioned, get parent directory wherein partitions are stored
        tb_dir = filepath.split(partitions)[0]
        create_or_append_table(dataset_id, table_id, tb_dir)
    else:
        create_or_append_table(dataset_id, table_id, filepath)

    # Delete local Files
    # log(f"Deleting local files: {raw_filepath}, {filepath}")
    # cleanup_local(filepath, raw_filepath)


@task
def upload_logs_to_bq(dataset_id, ref_table_id, timestamp, error):

    table_id = ref_table_id + "_logs"

    filepath = Path(
        f"{timestamp}/{table_id}/data={pendulum.parse(timestamp).date()}/{table_id}_{timestamp}.csv"
    )
    # create partition directory
    filepath.parent.mkdir(exist_ok=True, parents=True)
    # create dataframe to be uploaded
    dataframe = pd.DataFrame(
        {
            "timestamp_captura": [pd.to_datetime(timestamp)],
            "sucesso": [error is None],
            "erro": [error],
        }
    )
    # save local
    dataframe.to_csv(filepath, index=False)
    # BD Table object
    tb_obj = Table(dataset_id=dataset_id, table_id=table_id)
    # create and publish if table does not exist, append to it otherwise
    if not tb_obj.table_exists("staging"):
        tb_obj.create(
            path=f"{timestamp}/{table_id}",
            if_table_exists="replace",
            if_storage_data_exists="replace",
            if_table_config_exists="pass",
        )
    elif not tb_obj.table_exists("prod"):
        tb_obj.publish(if_exists="replace")
    else:
        tb_obj.append(filepath=f"{timestamp}/{table_id}", if_exists="replace")

    return tb_obj.table_exists("prod")

    # delete local file
    # shutil.rmtree(f"{timestamp}")
