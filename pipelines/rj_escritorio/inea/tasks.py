# -*- coding: utf-8 -*-
"""
Tasks for INEA.
"""
from datetime import datetime
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


@task
def fetch_vol_files(date: str, output_directory: str = "/var/escritoriodedados/temp/"):
    """
    Fetch files from INEA server

    Args:
        date (str): Date of the files to be fetched (e.g. 20220125)
    """
    log("Fetching files from INEA server...")
    # Creating temporary directory
    output_directory_path = Path(output_directory)
    output_directory_path.mkdir(parents=True, exist_ok=True)
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
    scp.get(fname, recursive=True, local_path=output_directory)
    # Close connection
    scp.close()


@task
def convert_vol_files(
    output_directory: str = "/var/escritoriodedados/temp/",
) -> List[str]:
    """
    Convert VOL files to NetCDF using the `volconvert` CLI tool.
    """
    # Start a list for converted files
    converted_files = []

    # List all files in the output directory
    output_directory_path = Path(output_directory)
    files = output_directory_path.glob("*.vol")
    total_files = len(list(files))
    log(f"Found {total_files} files to convert.")

    # Log each file and then delete it
    i = 0
    for file in files:
        log(f"Converting file {i+1}/{total_files} ({file}) to NetCDF...")
        # Run volconvert
        child = pexpect.spawn(
            f'/opt/edge/bin/volconvert {file} "NetCDF.'
            + '{-f=Whole -k=CFext -r=Short -p=Radar -M=All -z}"'
        )
        # Look for the "OutFiles:..." row and get only that row
        child.expect("OutFiles:(.*)\n")
        # Get the output file name
        converted_file = child.match.group(1).decode("utf-8").strip()
        # Add the file to the list
        converted_files.append(converted_file)
        # Log the output file name
        log(f"Output file: {converted_file}")
        # Go to the end of the command log
        child.expect(pexpect.EOF)
        # Delete the VOL file
        file.unlink()
        i += 1

    # Return the list of converted files
    return converted_files


@task
# pylint: disable=too-many-arguments, too-many-locals
def upload_files_to_gcs(
    converted_files: List[str],
    bucket_name: str,
    prefix: str,
    radar: str,
    product: str,
    mode="prod",
):
    """
    Upload files to GCS
    """
    # Assert all items in files_list are Path objects
    files_list: List[Path] = [Path(f) for f in converted_files]
    total_files = len(files_list)

    credentials = get_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    bucket = storage_client.bucket(bucket_name)

    for i, file in enumerate(files_list):
        if file.is_file():
            # Converted file path is in the format:
            # /var/opt/edge/.../YYYYMMDD/<filename>.nc.gz
            # We need to get the datetime for the file
            date_str = file.parent.name
            date = datetime.strptime(date_str, "%Y%m%d")
            blob_name = f"{prefix}/radar={radar}/produto={product}/data_particao={date}/{file.name}"
            blob_name = blob_name.replace("//", "/")
            log(f"Uploading file {i+1}/{total_files} ({file}) to GCS...")
            log(f"Blob name will be {blob_name}")
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(file)
            log(f"File {file} uploaded to GCS.")
            file.unlink()
