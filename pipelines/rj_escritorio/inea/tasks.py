# -*- coding: utf-8 -*-
"""
Tasks for INEA.
"""
from datetime import datetime, timedelta
from os import environ, getenv
from pathlib import Path
from typing import List

from google.cloud import storage
from paramiko import SSHClient
import pexpect
from prefect import task
from scp import SCPClient

from pipelines.utils.utils import log, get_credentials_from_env


@task
def print_environment_variables():
    """
    Print all environment variables
    """
    log("Environment variables:")
    for key, value in environ.items():
        log(f"{key}={value}")


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=30),
)
def fetch_vol_files(date: str, output_directory: str = "/var/escritoriodedados/temp/"):
    """
    Fetch files from INEA server

    Args:
        date (str): Date of the files to be fetched (e.g. 20220125)
    """
    log("Fetching files from INEA server...")
    # Creating temporary directory
    output_directory_path = Path(output_directory) / date
    output_directory_path.mkdir(parents=True, exist_ok=True)
    log(f"Temporary directory created: {output_directory_path}")
    # Get SSH password from env
    ssh_password = getenv("INEA_SSH_PASSWORD")
    # Open SSH client
    ssh_client = SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(hostname="a9921", username="root", password=ssh_password)
    # Open SCP client
    scp = SCPClient(ssh_client.get_transport(), sanitize=lambda x: x)
    # Fetch VOL files
    fname = f"/var/opt/edge/vols/9921GUA{date}*.vol"
    scp.get(fname, recursive=True, local_path=str(output_directory))
    # Close connection
    scp.close()
    # Return list of downloaded files
    downloaded_files = [str(f) for f in output_directory_path.glob("*.vol")]
    log(f"Downloaded files: {downloaded_files}")
    log(f"Found {len(downloaded_files)} files to convert.")
    return downloaded_files


@task
def convert_vol_file(
    downloaded_file: str,
    output_format: str = "NetCDF",
    convert_params: str = "-f=Whole -k=CFext -r=Short -p=Radar -M=All -z",
) -> List[str]:
    """
    Convert VOL files to NetCDF using the `volconvert` CLI tool.
    """
    # Run volconvert
    log(f"Converting file {downloaded_file} to {output_format}...")
    command = (
        f'/opt/edge/bin/volconvert {downloaded_file} "{output_format}.'
        + "{"
        + convert_params
        + '}"'
    )
    log(f"Running command: {command}")
    child = pexpect.spawn(command)
    try:
        log(f"before expect {str(child)}")
        # Look for the "OutFiles:..." row and get only that row
        child.expect("OutFiles:(.*)\n")
        # Get the output file name
        log(f"after expect {str(child)}")
        converted_file = child.match.group(1).decode("utf-8").strip()
        log(f"after match.group expect {str(child)}")
        # Log the output file name
        log(f"Output file: {converted_file}")
        # Go to the end of the command log
        child.expect(pexpect.EOF)
    except Exception as exc:
        # Log the error
        log(child.before.decode("utf-8"))
        raise exc
    # Delete the VOL file
    Path(downloaded_file).unlink()
    # Return the name of the converted file
    return converted_file


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=30),
)
# pylint: disable=too-many-arguments, too-many-locals
def upload_file_to_gcs(
    converted_file: str,
    bucket_name: str,
    prefix: str,
    radar: str,
    product: str,
    mode="prod",
):
    """
    Upload files to GCS
    """
    credentials = get_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    bucket = storage_client.bucket(bucket_name)

    file = Path(converted_file)
    if file.is_file():
        # Converted file path is in the format:
        # /var/opt/edge/.../YYYYMMDD/<filename>.nc.gz
        # We need to get the datetime for the file
        date_str = file.parent.name
        date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        blob_name = (
            f"{prefix}/radar={radar}/produto={product}/data_particao={date}/{file.name}"
        )
        blob_name = blob_name.replace("//", "/")
        log(f"Uploading file {file} to GCS...")
        log(f"Blob name will be {blob_name}")
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file)
        log(f"File {file} uploaded to GCS.")
        file.unlink()


@task
def execute_shell_command(command: str):
    """
    Executes a shell command and logs output
    """
    log(f"Executing command: {command}")
    child = pexpect.spawn(command)
    child.expect(pexpect.EOF)
    log(child.before.decode("utf-8"))
