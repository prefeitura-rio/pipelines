# -*- coding: utf-8 -*-
"""
Tasks to dump data from a INEA FTP to BigQuery
"""
# pylint: disable=E0702,E1137,E1136,E1101,C0207,W0613
from datetime import datetime, timedelta
from pathlib import Path

from google.cloud import storage
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped

from pipelines.utils.ftp.client import FTPClient
from pipelines.utils.utils import (
    log,
    get_credentials_from_env,
    get_vault_secret,
)


@task
def get_ftp_client(wait=None):
    """
    Get FTP client
    """
    inea_secret = get_vault_secret("ftp_inea_radar")
    hostname = inea_secret["data"]["hostname"]
    username = inea_secret["data"]["username"]
    password = inea_secret["data"]["password"]

    return FTPClient(
        hostname=hostname,
        username=username,
        password=password,
    )


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=30),
)
def get_files_to_download(client, radar, redis_files):
    """
    Get files to download FTP and GCS
    """

    client.connect()
    files = client.list_files(path=f"./{radar.upper()}/")
    log(f"\n\nAvailable files on FTP: {files}")
    log(f"\nFiles already saved on redis_files: {redis_files}")
    files = [file for file in files if file not in redis_files]
    log(f"\nFiles to be downloaded: {files}")
    files = files[-4:]  # remover
    log(f"\nFiles to be downloaded: {files}")

    # Skip task if there is no new file
    if len(files) == 0:
        log("No new available files")
        skip = Skipped("No new available files")
        raise ENDRUN(state=skip)

    return files


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=30),
)
def download_files(client, files, radar):
    """
    Download files from FTP
    """

    save_path = Path(radar.upper())
    save_path.mkdir(parents=True, exist_ok=True)

    client.connect()
    files_downloaded = []
    for file in files:
        # file_path = save_path / file
        file_path = file
        client.download(remote_path=file, local_path=file_path)
        files_downloaded.append(file_path)
    log(f"files_downloaded: {files_downloaded}")
    file = Path(files_downloaded[0])
    log(f"DEBUGGGG: {file.name.split('-')[2]}")
    return files_downloaded


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=30),
)
# pylint: disable=too-many-arguments, too-many-locals
def upload_file_to_gcs(
    file_to_upload: str,
    bucket_name: str,
    prefix: str,
    radar: str,
    product: str,
    mode="prod",
    task_mode="partitioned",
    unlink: bool = True,
):
    """
    Upload files to GCS
    """
    credentials = get_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    bucket = storage_client.bucket(bucket_name)

    file = Path(file_to_upload)
    if task_mode == "partitioned":
        log(f"DEBUG: {file} e {file.name}")
        date_str = file.name.split("-")[2]
        date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        blob_name = (
            f"{prefix}/radar={radar}/produto={product}/data_particao={date}/{file.name}"
        )
        blob_name = blob_name.replace("//", "/")
    elif task_mode == "raw":
        blob_name = f"{prefix}/{file.name}"

    log(f"Uploading file {file} to GCS...")
    log(f"Blob name will be {blob_name}")
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file)
    log(f"File {file} uploaded to GCS.")
    if unlink:
        file.unlink()
