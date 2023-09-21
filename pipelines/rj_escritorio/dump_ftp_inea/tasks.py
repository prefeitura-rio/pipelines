# -*- coding: utf-8 -*-
"""
Tasks to dump data from a INEA FTP to BigQuery
"""
# pylint: disable=E0702,E1137,E1136,E1101,C0207,W0613
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

from google.cloud import storage
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped

from pipelines.utils.ftp.client import FTPClient
from pipelines.utils.utils import (
    log,
    get_credentials_from_env,
    get_vault_secret,
    list_blobs_with_prefix,
)


@task(
    nout=2,
    max_retries=2,
    retry_delay=timedelta(seconds=10),
)
# pylint: disable=too-many-arguments,too-many-locals, too-many-branches
def get_files_datalake(
    bucket_name: str,
    prefix: str,
    radar: str,
    product: str,
    date: str = None,
    greater_than: str = None,
    mode: str = "prod",
) -> Tuple[List[str], str]:
    """
    List files from INEA server

    Args:
        product (str): "ppi"
        date (str): Date of the files to be fetched (e.g. 2022-01-25)
        greater_than (str): Fetch files with a date greater than this one
        less_than (str): Fetch files with a date less than this one
        output_directory (str): Directory where the files will be saved
        radar (str): Radar name. Must be `gua` or `mac`
        get_only_last_file (bool): Treat only the last file available

    How to use:
        to get real time data:
            let `greater_than` and `date` as None and `get_only_last_file` as True
            This will prevent the flow to be stucked treating all files when something happend
            and stoped the flow. Otherwise the flow will take a long time to treat all files
            and came back to real time.
        to fill missing files up to two days ago:
            let `greater_than` and `date` as None and `get_only_last_file` as False
        for backfill or to fill missing files for dates greather than two days ago:
            add a `greater_than` date and let `date` as None and `get_only_last_file` as False
        get all files for one day
            let `greater_than` as None and `get_only_last_file` as False and fill `date`
    """
    search_prefix = f"{prefix}/radar={radar}/produto={product}"

    # Get today's blobs
    current_date = datetime.now().date()
    current_date_str = current_date.strftime("%Y-%m-%d")
    blobs = list_blobs_with_prefix(
        bucket_name=bucket_name,
        prefix=f"{search_prefix}/data_particao={current_date_str}",
        mode=mode,
    )
    log(
        f"Searched for blobs with prefix {search_prefix}/data_particao={current_date_str}"
    )

    if greater_than is None:
        past_date = current_date - timedelta(days=1)
    else:
        past_date = datetime.strptime(greater_than, "%Y-%m-%d")
        past_date = past_date.date()

    # Next, we get past day's blobs
    while past_date < current_date:
        past_date_str = past_date.strftime("%Y-%m-%d")
        past_blobs = list_blobs_with_prefix(
            bucket_name=bucket_name,
            prefix=f"{search_prefix}/data_particao={past_date_str}",
            mode=mode,
        )
        log(
            f"Searched for blobs with prefix {search_prefix}/data_particao={past_date_str}"
        )
        # Then, we merge the two lists
        blobs += past_blobs
        past_date += timedelta(days=1)

    # Now, we sort it by `blob.name`
    blobs.sort(key=lambda blob: blob.name)
    # Get only the filenames
    datalake_files = [blob.name.split("/")[-1] for blob in blobs]
    # Format of the name is 9921GUA-PPIVol-20220930-121010-0004.hdf
    # We need remove the last characters to stay with 9921GUA-PPIVol-20220930-121010
    datalake_files = ["-".join(fname.split("-")[:-1]) for fname in datalake_files]

    return datalake_files


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
def get_files_to_download(
    client,
    radar,
    redis_files,
    datalake_files,
    get_only_last_file: bool = True,
):
    """
    Get files to download FTP and GCS
    """

    client.connect()
    files = client.list_files(path=f"./{radar.upper()}/")
    log(f"\n\nAvailable files on FTP: {files}")
    log(f"\nFiles already saved on redis_files: {redis_files}")
    # Files obtained direct from INEA ends with 0000 as "9915MAC-PPIVol-20230921-123000-0000.hdf"
    # Files from FTP ends with an alphanumeric string as "9915MAC-PPIVol-20230921-142000-54d4.hdf"
    # We need to be careful when changing one pipeline to other
    # Check if files are already on redis
    files = [file for file in files if file not in redis_files]

    # Check if files are already on datalake
    if len(datalake_files) > 0:
        files = [
            file
            for file in files
            if "-".join(file.split("-")[:-1]) not in datalake_files
        ]

    # Skip task if there is no new file
    if len(files) == 0:
        log("No new available files")
        skip = Skipped("No new available files")
        raise ENDRUN(state=skip)

    files.sort()

    log(f"\nFiles to be downloaded: {files}")
    if len(files) > 20:
        files = files[-20:]  # remover

    if get_only_last_file:
        files = files[-1]
    log(f"\nFiles to be downloaded: {files}")

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
