# -*- coding: utf-8 -*-
# pylint: disable=W0612, W0613, W0102, W1514
# flake8: noqa: F841
"""
Tasks for setting rain dashboard using radar data.
"""
from datetime import timedelta
import json
import os
from pathlib import Path
from typing import Union, Tuple

import pandas as pd
import pendulum
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.radar.precipitacao.utils import (
    download_blob,
    list_blobs_with_prefix,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.src.predict_rain import (
    run_model_prediction,
)
from pipelines.utils.utils import log, parse_date_columns, to_partitions


@task()
def get_filenames_storage(
    bucket_name: str = "rj-escritorio-dev",
    radar: str = "gua",
    files_saved_redis: list = [],
) -> list:
    """Esc"""
    last_30min = pendulum.now("UTC").subtract(minutes=30).to_datetime_string()
    today = pendulum.now("UTC").format("YYYY-MM-DD")

    # Get data from yesterday if the date from last_30min is different from today
    if today == last_30min[:11]:
        filter_partition_days = [today]
    else:
        filter_partition_days = [last_30min[:11], today]

    files_on_storage_list = []
    for i in filter_partition_days:
        base_path = "raw/meio_ambiente_clima/inea_radar_hdf5/"
        prefix = base_path + f"radar={radar}/produto=ppi/data_particao={i}/"
        files_on_storage = list_blobs_with_prefix(bucket_name, prefix, delimiter=None)
        files_on_storage_list.extend([blob.name for blob in files_on_storage])

    files_on_storage_list = list(set(files_on_storage_list))
    files_on_storage_list.sort()
    files_on_storage_list = files_on_storage_list[-3:]

    files_saved_redis.sort()

    log(f"[DEBUG] Last radar files: {files_on_storage_list}")
    log(f"[DEBUG] Last redis files: {files_saved_redis}")

    # if we have the same files on redis and on radar, skip flow
    if files_on_storage_list == files_saved_redis:
        log("No available files on API")
        skip = Skipped("No available files on API")
        raise ENDRUN(state=skip)

    return files_on_storage_list


@task()
def download_files_storage(
    bucket_name: str, files_to_download: list, destination_path: str
) -> None:
    """
    Realiza o download dos arquivos listados em files_to_download no bucket especificado
    """

    os.makedirs(destination_path, exist_ok=True)

    for file in files_to_download:
        source_blob_name, destination_file_name = file, file.split("/")[-1]
        destination_file_name = Path(destination_path, destination_file_name)
        download_blob(bucket_name, source_blob_name, destination_file_name)


@task()
def change_predict_rain_specs(files_to_model: list, destination_path: str) -> None:
    """
    Change name of radar files inside json file
    """
    json_file = (
        "pipelines/rj_cor/meteorologia/radar/precipitacao/src/predict_rain_specs.json"
    )
    with open(json_file, "r") as file:
        predict_specs = json.load(file)
    log("load")
    filenames = [destination_path + i.split("/")[-1] for i in files_to_model]
    log(f"filenames to save on json file: {filenames}")
    predict_specs["radar_ppi_hdfs"] = filenames
    log(f"predict_specs : {predict_specs}")
    with open(json_file, "w") as file:
        json.dump(predict_specs, file)

    # verifica arquivo
    with open(json_file, "r") as file:
        predict_specs_valida = json.load(file)
    log(f"predict_specs como ficou: {predict_specs_valida}")


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def run_model(wait=None) -> Tuple[pd.DataFrame, Union[str, Path]]:
    """
    Call a shell task to run model
    https://github.com/BioBD/sgwfc-gene-python/blob/7dadf7b854a7a37405ee203331671f8cd61b114b/workflow/modules.py
    """
    log("[DEBUG] Start runing model")
    base_path = "pipelines/rj_cor/meteorologia/radar/precipitacao"
    data_path = f"{base_path}/predictions/"
    path = Path(data_path)
    if path.exists():
        log("DEBUG predictions path exists")
    else:
        os.makedirs(data_path, exist_ok=True)

    dfr = run_model_prediction(base_path=base_path)

    log("[DEBUG] End runing model")

    return dfr


@task
def save_data(dfr: pd.DataFrame) -> Union[str, Path]:
    """
    Save treated data in csv partitioned by date
    """

    prepath = Path("/tmp/precipitacao_radar/")
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data_medicao"
    dataframe, partitions = parse_date_columns(dfr, partition_column)
    suffix = pd.to_datetime(dataframe[partition_column]).max().strftime("%Y%m%d%H%M%S")

    # Cria partições a partir da data
    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=suffix,
    )
    log(f"[DEBUG] Files saved on {prepath}")
    return prepath
