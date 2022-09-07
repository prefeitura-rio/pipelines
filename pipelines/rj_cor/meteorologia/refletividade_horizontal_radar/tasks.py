# -*- coding: utf-8 -*-
# pylint: disable=W0613
"""
Tasks for meteorologia/refletividade_horizontal_radar
"""
from datetime import timedelta
import ftplib
import os
import socket

from prefect import task
from prefect.triggers import all_successful

from pipelines.constants import constants
from pipelines.rj_cor.comando.eventos.utils import build_redis_key
from pipelines.rj_cor.meteorologia.refletividade_horizontal_radar.utils import (
    upload_blob,
)
from pipelines.utils.utils import get_redis_client, get_vault_secret, log


@task(
    nout=1,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download(files_on_redis: list) -> str:
    """
    Faz o request e salva dados localmente
    """
    # Acessar FTP ftp_inea_radar
    dicionario = get_vault_secret("ftp_inea_radar")
    host = dicionario["data"]["host"]
    username = dicionario["data"]["username"]
    password = dicionario["data"]["password"]

    # Cria pasta para salvar arquivo de download
    base_path = os.path.join(os.getcwd(), "data", "refletividade_horizontal", "input")

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    try:
        ftp = ftplib.FTP(host)
    except (socket.error, socket.gaierror):
        log(f"ERROR: cannot reach {host}")
        raise
    log(f"*** Connected to host {host}")

    try:
        ftp.login(username, password)
    except ftplib.error_perm:
        log("ERROR: cannot login")
        ftp.quit()
        raise
    log("*** Logged in successfully")

    for radar in ["macae", "guaratiba"]:
        dirname = f"/{radar}/cmax/"

        files = ftp.nlst(dirname)
        files = [i for i in files if i.endswith(".nc")]

        log(f"*** Got filenames in {dirname}")

        try:
            for file_path in files:
                filename = file_path.split("/")[-1]
                if filename not in files_on_redis:
                    save_on = os.path.join(base_path, filename)
                    with open(save_on, "wb") as file:
                        log("Getting " + file_path)
                        ftp.retrbinary("RETR %s" % file_path, file.write)
                    log(f"File downloaded to {save_on}")
        except ftplib.error_perm:
            log(f"ERROR: cannot read file {file_path}")
            raise

    ftp.quit()

    return base_path


@task(nout=1)
def upload_gcp(
    dataset_id: str,
    table_id: str,
    local_files_path: str,
    files_on_redis: list,
    wait=None,
):
    """
    Faz o upload dos dados no gcp
    """
    bucket_name = "rj-cor"
    destination_blob_name_prefix = f"staging/{dataset_id}/{table_id}/"

    # local_files_path = 'data/refletividade_horizontal/input/'
    files_on_ftp = os.listdir(local_files_path)
    files_on_ftp = list(set(files_on_ftp))
    files_to_upload = [
        i for i in files_on_ftp if i.endswith(".nc") if i not in files_on_redis
    ]
    print(">>>>> files to upload", files_to_upload)

    for file in files_to_upload:
        destination_blob_name = destination_blob_name_prefix + file
        file_path = os.path.join(local_files_path, file)
        log(f"Starting {file} upload on {destination_blob_name}")
        upload_blob(bucket_name, file_path, destination_blob_name)

    return files_on_ftp


@task
def get_on_redis(
    dataset_id: str,
    table_id: str,
    mode: str = "prod",
) -> list:
    """
    Set the last updated time on Redis.
    """
    redis_client = get_redis_client()
    key = build_redis_key(dataset_id, table_id, "files", mode)
    files_on_redis = redis_client.get(key)
    print(">>>>>>> files_on_redis", files_on_redis)
    files_on_redis = [] if files_on_redis is None else files_on_redis
    print(">>>>>>> files_on_redis", files_on_redis)
    files_on_redis = list(set(files_on_redis))
    return files_on_redis


@task(trigger=all_successful)
def save_on_redis(
    dataset_id: str, table_id: str, mode: str = "prod", files: list = None, wait=None
) -> None:
    """
    Set the last updated time on Redis.
    """
    redis_client = get_redis_client()
    key = build_redis_key(dataset_id, table_id, "files", mode)
    redis_client.set(key, files)
