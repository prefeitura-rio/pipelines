# -*- coding: utf-8 -*-
"""
Tasks to dump data from a INEA FTP to BigQuery
"""
# pylint: disable=E0702,E1137,E1136,E1101,W0613,bad-continuation
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

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


@task(nout=2, max_retries=2, retry_delay=timedelta(seconds=10))
# pylint: disable=too-many-arguments,too-many-locals, too-many-branches
def get_files_datalake(
    bucket_name: str,
    prefix: str,
    radar: str,
    product: str,
    date: str = None,
    greater_than: str = None,
    check_datalake_files: bool = True,
    mode: str = "prod",
) -> List[str]:
    """
    List files from INEA saved on datalake

    Args:
        product (str): "ppi"
        date (str): Date of the files to be fetched (e.g. 2022-01-25)
        greater_than (str): Fetch files with a date greater than this one (e.g. 2022-01-25)
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

    if check_datalake_files:
        search_prefix = f"{prefix}/radar={radar}/produto={product}"

        # Get today's blobs
        if date:
            current_date = datetime.strptime(date, "%Y-%m-%d")
        else:
            current_date = datetime.now().date()

        if greater_than is None:
            past_date = current_date - timedelta(days=1)
        else:
            past_date = datetime.strptime(greater_than, "%Y-%m-%d")
            past_date = past_date.date()

        blobs = []
        # Next, we get past day's blobs
        while past_date <= current_date:
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
        log(f"Last 5 datalake files: {datalake_files[-5:]}")

    else:
        datalake_files = []
        log("This run is not considering datalake files")

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

    return FTPClient(hostname=hostname, username=username, password=password)


@task(max_retries=3, retry_delay=timedelta(seconds=30))
# pylint: disable=too-many-arguments
def get_files_to_download(
    client,
    radar: str,
    redis_files: list,
    datalake_files: list,
    date: str = None,
    greater_than: str = None,
    get_only_last_file: bool = True,
) -> List[str]:
    """
    List and get files to download FTP

    Args:
        radar (str): Radar name. Must be `gua` or `mac`
        redis_files (list): List with last files saved on GCP and redis
        datalake_files (list): List with filenames saved on GCP
        date (str): Date of the files to be fetched (e.g. 2022-01-25)
        greater_than (str): Fetch files with a date greater than this one (e.g. 2022-01-25)
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

    client.connect()
    files = client.list_files(path=f"./{radar.upper()}/")
    # log(f"\n\nAvailable files on FTP: {files}")
    # log(f"\nFiles already saved on redis_files: {redis_files}")

    # Files obtained direct from INEA ends with 0000 as "9915MAC-PPIVol-20230921-123000-0000.hdf"
    # Files from FTP ends with an alphanumeric string as "9915MAC-PPIVol-20230921-142000-54d4.hdf"
    # We need to be careful when changing one pipeline to other

    # Get specific files based on date and greater_than parameters
    if date:
        files = [file for file in files if file.split("-")[2] == date.replace("-", "")]
    if greater_than:
        files = [
            file
            for file in files
            if file.split("-")[2] >= greater_than.replace("-", "")
        ]

    # Check if files are already on redis
    files = [file for file in files if file not in redis_files]

    # Check if files are already on datalake
    # Some datalake files use the pattern "9915MAC-PPIVol-20230921-123000-0000.hdf"
    # Files from FTP use the pattern "./MAC/9915MAC-PPIVol-20230921-123000-3f28.hdf"
    # We are going to compare "9915MAC-PPIVol-20230921-123000" from both places
    if len(datalake_files) > 0:
        log("Removing files that are already on datalake")
        files = [
            file
            for file in files
            if "-".join(file.split("/")[-1].split("-")[:-1])
            not in ["-".join(dfile.split("-")[:-1]) for dfile in datalake_files]
        ]

    # Skip task if there is no new file
    if len(files) == 0:
        log("No new available files")
        skip = Skipped("No new available files")
        raise ENDRUN(state=skip)

    files.sort()

    if get_only_last_file:
        files = [files[-1]]
    log(f"\nFiles to be downloaded: {files}")
    return files


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def download_files(client, files, radar) -> List[str]:
    """
    Download files from FTP
    """

    save_path = Path(radar.upper())
    save_path.mkdir(parents=True, exist_ok=True)

    client.connect()
    files_downloaded = []
    for file in files:
        log(f"Downloading file: {file}")
        # file_path = save_path / file
        file_path = file
        client.download(remote_path=file, local_path=file_path)
        files_downloaded.append(file_path)
    log(f"Downloaded: {files_downloaded}")
    file = Path(files_downloaded[0])
    log(f"DEBUGGGG: {file.name.split('-')[2]}")
    return files_downloaded


@task(max_retries=3, retry_delay=timedelta(seconds=30))
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
