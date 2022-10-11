# -*- coding: utf-8 -*-
"""
Tasks for INEA.
"""
from datetime import datetime, timedelta
from os import environ, getenv
from pathlib import Path
import subprocess
from typing import Callable, List
from uuid import uuid4

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
def build_regex_expression(
    greater_than: str,
    working_directory: str = "/var/opt/edge/vols",
    filename_prefix: str = "9921GUA",
    file_extension: str = "vol",
):
    """
    Builds the regex expression for including only the files that we want to
    download. The format is the following:

    <working_directory>/<filename_prefix><greater_than>.<file_extension>

    `greater_than` must be numeric only.
    """
    if greater_than is None:
        return ""
    # Assert that greater_than is numeric
    for char in greater_than:
        assert (
            char in "0123456789"
        ), f"{char} is not numeric. greater_than must be numeric only."
    # Add trailing slash
    working_directory = (
        f"{working_directory}/"
        if not working_directory.endswith("/")
        else working_directory
    )
    # Build initial expression
    expression = f"{working_directory}{filename_prefix}"
    # Adds greater than expression
    for char in greater_than:
        expression += f"[{char}-9]"
    # Adds anything after greater than expression
    expression += ".*"
    # Adds file extension
    expression += f"{file_extension}"
    log(f"Regex expression: {expression}")
    return expression


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=30),
)
def fetch_vol_files(
    date: str,
    regex_expression: str = "",
    output_directory: str = "/var/escritoriodedados/temp/",
):
    """
    Fetch files from INEA server

    Args:
        date (str): Date of the files to be fetched (e.g. 20220125)
    """

    def run_shell_command(command: str, stdout_callback: Callable = log) -> int:
        popen = subprocess.Popen(
            command, shell=True, stdout=subprocess.PIPE, universal_newlines=True
        )
        for stdout_line in iter(popen.stdout.readline, ""):
            stdout_callback(stdout_line)
        popen.stdout.close()
        return_code = popen.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, command)
        return return_code

    # Either date or regex_expression must be provided
    assert date or regex_expression, "Either date or regex_expression must be provided."

    log("Fetching files from INEA server...")
    # Creating temporary directory
    if date:
        output_directory_path = Path(output_directory) / date
    else:
        output_directory_path = Path(output_directory) / f"regex-{uuid4()}"
    output_directory_path.mkdir(parents=True, exist_ok=True)
    log(f"Temporary directory created: {output_directory_path}")
    # Get SSH password from env
    ssh_password = getenv("INEA_SSH_PASSWORD")
    # Open SSH client
    ssh_client = SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(hostname="a9921", username="root", password=ssh_password)
    # List remote files
    log("Listing remote files...")
    if date:
        _, stdout, _ = ssh_client.exec_command(
            f"find /var/opt/edge/vols -name '9921GUA{date}*.vol'"
        )
    else:
        _, stdout, _ = ssh_client.exec_command(
            f"find /var/opt/edge/vols -name '{regex_expression}'"
        )
    remote_files = stdout.read().decode("utf-8").splitlines()
    log(f"Found {len(remote_files)} files.")
    log(f"Remote files: {remote_files}")
    raise ValueError("Test")
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
    task_mode="partitioned",
    unlink: bool = True,
):
    """
    Upload files to GCS
    """
    credentials = get_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    bucket = storage_client.bucket(bucket_name)

    file = Path(converted_file)
    if file.is_file():
        if task_mode == "partitioned":
            # Converted file path is in the format:
            # /var/opt/edge/.../YYYYMMDD/<filename>.nc.gz
            # We need to get the datetime for the file
            date_str = file.parent.name
            date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
            blob_name = f"{prefix}/radar={radar}/produto={product}/data_particao={date}/{file.name}"
            blob_name = blob_name.replace("//", "/")
        elif task_mode == "raw":
            blob_name = f"{prefix}/{file.name}"
        else:
            raise ValueError(f"Invalid task_mode: {task_mode}")
        log(f"Uploading file {file} to GCS...")
        log(f"Blob name will be {blob_name}")
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file)
        log(f"File {file} uploaded to GCS.")
        if unlink:
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
