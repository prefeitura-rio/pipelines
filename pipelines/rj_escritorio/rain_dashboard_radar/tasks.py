# -*- coding: utf-8 -*-
# pylint: disable=W1514, W0612
# flake8: noqa: F841
"""
Tasks for setting rain dashboard using radar data.
"""
import json
from pathlib import Path
import os
import pendulum
from prefect import task
from prefect.tasks.shell import ShellTask

from pipelines.rj_escritorio.rain_dashboard_radar.utils import (
    download_blob,
    list_blobs_with_prefix,
)
from pipelines.utils.utils import log


@task()
def get_filenames_storage(
    bucket_name: str = "rj-escritorio-dev", radar: str = "gua"
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
    log(f"[DEBUG] Last radar files: {files_on_storage_list[-3:]}")

    return files_on_storage_list[-3:]


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
        "pipelines/rj_escritorio/rain_dashboard_radar/src/predict_rain_specs.json"
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


@task()
def run_model():
    """
    Call a shell task to run model
    """
    log("[DEBUG] Start runing model")
    base_path = "pipelines/rj_escritorio/rain_dashboard_radar"
    shell_task = ShellTask(
        name="Run model",
        command=f"python {base_path}/src/predict_rain.py -sf {base_path}/src/predict_specs.json",
    )
    log("[DEBUG] End runing model")
