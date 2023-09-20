# -*- coding: utf-8 -*-
"""
Tasks to dump data from a INEA FTP to BigQuery
"""
# pylint: disable=E0702,E1137,E1136,E1101,C0207,W0613
from datetime import datetime, timedelta
from pathlib import Path

from google.cloud import storage
from prefect import task

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
def get_files_to_download(client, radar):
    """
    Get files to download FTP and GCS
    """

    client.connect()
    files = client.list_files(path=f"./{radar.upper()}/")
    files = files[-4:]
    log(f"files: {files}")

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
    log(f"DEBUGGGG: {file.name}")
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

    if task_mode == "partitioned":
        # We need to get the datetime for the file
        log(f"DEBUG: {file_to_upload} e {file_to_upload.name}")
        date_str = file_to_upload.split("-")[2]
        date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        blob_name = (
            f"{prefix}/radar={radar}/produto={product}/"
            f"data_particao={date}/{file_to_upload.name}"
        )
        blob_name = blob_name.replace("//", "/")
    elif task_mode == "raw":
        blob_name = f"{prefix}/{file_to_upload.name}"
        log(f"Uploading file {file_to_upload} to GCS...")
        log(f"Blob name will be {blob_name}")
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_to_upload)
        log(f"File {file_to_upload} uploaded to GCS.")
        if unlink:
            file_to_upload.unlink()
