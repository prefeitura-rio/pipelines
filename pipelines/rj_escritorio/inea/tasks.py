# -*- coding: utf-8 -*-
"""
Tasks for INEA.
"""
from datetime import datetime, timedelta
from functools import partial
from os import environ, getenv
from pathlib import Path
import subprocess
from typing import Callable, List, Tuple

from google.cloud import storage
from paramiko import SSHClient
import pexpect
from prefect import task
from scp import SCPClient

from pipelines.utils.utils import get_credentials_from_env, list_blobs_with_prefix, log


@task
def print_environment_variables():
    """
    Print all environment variables
    """
    log("Environment variables:")
    for key, value in environ.items():
        log(f"{key}={value}")


@task(
    nout=2,
    max_retries=2,
    retry_delay=timedelta(seconds=10),
)
def list_vol_files(
    bucket_name: str,
    prefix: str,
    radar: str,
    product: str,
    date: str = None,
    greater_than: str = None,
    mode: str = "prod",
    output_format: str = "NetCDF",
    output_directory: str = "/var/escritoriodedados/temp/",
) -> Tuple[List[str], str]:
    """
    List files from INEA server

    Args:
        date (str): Date of the files to be fetched (e.g. 20220125)
        greater_than (str): Fetch files with a date greater than this one
        output_directory (str): Directory where the files will be saved
    """

    # If none of `date` or `greater_than` are provided, find blob with the latest date
    if date is None and greater_than is None:
        # First, we build the search prefix
        search_prefix = f"{prefix}/radar={radar}/produto={product}"
        # Then, we add the current date partition
        current_date = datetime.now()
        current_date_str = current_date.strftime("%Y-%m-%d")
        today_blobs = list_blobs_with_prefix(
            bucket_name=bucket_name,
            prefix=f"{search_prefix}/data_particao={current_date_str}",
            mode=mode,
        )
        # Next, we get past day blobs
        past_date = current_date - timedelta(days=1)
        past_date_str = past_date.strftime("%Y-%m-%d")
        past_blobs = list_blobs_with_prefix(
            bucket_name=bucket_name,
            prefix=f"{search_prefix}/data_particao={past_date_str}",
            mode=mode,
        )
        # Then, we merge the two lists
        blobs = today_blobs + past_blobs
        # Now, we sort it by `blob.name`
        blobs.sort(key=lambda blob: blob.name)
        # Finally, we get the latest blob
        latest_blob = blobs[-1]
        # And we get the greater_than from its name (differs for every output_format)
        if output_format == "NetCDF":
            # Format of the name is 9921GUA-20221017-070010-PPIVol-0000.nc.gz
            # We need to join 20221017 and 070010
            greater_than = (
                latest_blob.name.split("-")[1] + latest_blob.name.split("-")[2]
            )
        elif output_format == "HDF5":
            # Format of the name is 9921GUA-PPIVol-20220930-121010-0004.hdf
            # We need to join 20220930 and 121010
            greater_than = (
                latest_blob.name.split("-")[2] + latest_blob.name.split("-")[3]
            )
        log(f"Latest blob date: {greater_than}")

    raise ValueError("Not implemented yet")

    # Creating temporary directory
    if date:
        output_directory_path = Path(output_directory) / date
    else:
        output_directory_path = Path(output_directory) / f"greaterthan-{greater_than}"
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
        remote_files = stdout.read().decode("utf-8").splitlines()
    else:
        _, stdout, _ = ssh_client.exec_command(
            "find /var/opt/edge/vols -name '9921GUA*.vol'"
        )
        all_files = stdout.read().decode("utf-8").splitlines()
        remote_files = [
            file
            for file in all_files
            if file.split("/")[-1][: len(greater_than) + 7] >= f"9921GUA{greater_than}"
        ]
    log(f"Found {len(remote_files)} files.")
    log(f"Remote files: {remote_files}")
    return remote_files, output_directory_path


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=30),
)
def fetch_vol_file(
    remote_file: str,
    output_directory: str = "/var/escritoriodedados/temp/",
):
    """
    Fetch files from INEA server

    Args:
        remote_file (str): Remote file to be fetched
        output_directory (str): Directory where the files will be saved
    """
    # Get SSH password from env
    ssh_password = getenv("INEA_SSH_PASSWORD")

    # Open SSH client
    ssh_client = SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(hostname="a9921", username="root", password=ssh_password)

    # Open SCP client
    scp = SCPClient(ssh_client.get_transport(), sanitize=lambda x: x)

    # Fetch VOL file
    scp.get(remote_file, local_path=str(output_directory))

    # Close connection
    scp.close()

    # Return local file path
    return Path(output_directory) / remote_file.split("/")[-1]


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
def execute_shell_command(
    command: str,
    stdout_callback: Callable = log,
    stderr_callback: Callable = partial(log, level="error"),
):
    """
    Executes a shell command and logs output
    """
    popen = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        universal_newlines=True,
        stderr=subprocess.PIPE,
    )
    for stdout_line in iter(popen.stdout.readline, ""):
        stdout_callback(stdout_line)
    for stderr_line in iter(popen.stderr.readline, ""):
        stderr_callback(stderr_line)
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        log(f"Command {command} failed with return code {return_code}", "error")
    else:
        log(f"Command {command} executed successfully")
