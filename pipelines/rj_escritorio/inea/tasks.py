# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0915

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
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped
from scp import SCPClient

from pipelines.utils.utils import (
    get_credentials_from_env,
    get_vault_secret,
    list_blobs_with_prefix,
    log,
)


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
# pylint: disable=too-many-arguments,too-many-locals
def list_vol_files(
    bucket_name: str,
    prefix: str,
    radar: str,
    product: str,
    date: str = None,
    greater_than: str = None,
    # less_than: str = None,
    get_only_last_file: bool = True,
    mode: str = "prod",
    output_format: str = "HDF5",
    output_directory: str = "/var/escritoriodedados/temp/",
    vols_remote_directory: str = "/var/opt/edge/vols",
) -> Tuple[List[str], str]:
    """
    List files from INEA server

    Args:
        product (str): "ppi"
        date (str): Date of the files to be fetched (e.g. 20220125)
        greater_than (str): Fetch files with a date greater than this one
        less_than (str): Fetch files with a date less than this one
        output_format (str): "NetCDF" or "HDF5"
        output_directory (str): Directory where the files will be saved
        radar (str): Radar name. Must be `gua` or `mac`
        get_only_last_file (bool): Treat only the last file available
    """

    # If none of `date`, `greater_than` or `less_than` are provided, find blob with the latest date
    # if date is None and greater_than is None and less_than is None:
    if date is None and greater_than is None:
        log("No date or greater_than provided. Finding latest blob...")
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
        log(
            f"Searched for blobs with prefix {search_prefix}/data_particao={current_date_str}"
        )
        # Next, we get past day blobs
        past_date = current_date - timedelta(days=1)
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
        blobs = today_blobs + past_blobs
        # Now, we sort it by `blob.name`
        blobs.sort(key=lambda blob: blob.name)
        # Finally, we get the latest blob
        latest_blob = blobs[-1]
        log(f"Latest blob found: {latest_blob.name}")
        # And we get the greater_than from its name (differs for every output_format)
        if output_format == "NetCDF":
            # Format of the name is 9921GUA-20221017-070010-PPIVol-0000.nc.gz
            # We need to join 20221017 and 070010
            fname = latest_blob.name.split("/")[-1]
            greater_than = fname.split("-")[1] + fname.split("-")[2]
        elif output_format == "HDF5":
            # Format of the name is 9921GUA-PPIVol-20220930-121010-0004.hdf
            # We need to join 20220930 and 121010
            fname = latest_blob.name.split("/")[-1]
            greater_than = fname.split("-")[2] + fname.split("-")[3]
        log(f"Latest blob date: {greater_than}")

    # Creating temporary directory
    if date:
        output_directory_path = Path(output_directory) / date
    else:
        output_directory_path = Path(output_directory) / f"greaterthan-{greater_than}"
    output_directory_path.mkdir(parents=True, exist_ok=True)
    log(f"Temporary directory created: {output_directory_path}")

    # Create vars based on radar name
    if radar == "gua":
        env_variable = "INEA_SSH_PASSWORD"
        hostname = "a9921"
        startswith = "9921GUA"
    elif radar == "mac":
        dicionario = get_vault_secret("inea_mac_ssh_password")
        env_variable = dicionario["data"]["password"]
        hostname = "a9915"
        startswith = "9915MAC"

    # Get SSH password from env
    ssh_password = getenv(env_variable)

    # Open SSH client
    ssh_client = SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(
        hostname=hostname,
        username="root",
        password=ssh_password,
        timeout=300,
        auth_timeout=300,
        banner_timeout=300,
    )

    # List remote files
    log(f"Listing remote files for radar {startswith}...")
    if date:
        _, stdout, _ = ssh_client.exec_command(
            f"find {vols_remote_directory} -name '{startswith}{date}*.vol'"
        )
        remote_files = stdout.read().decode("utf-8").splitlines()
        if len(remote_files) == 0:
            _, stdout, _ = ssh_client.exec_command(
                f"find {vols_remote_directory} -name '*{startswith}*.vol'"
            )
            remote_files = stdout.read().decode("utf-8").splitlines()
            remote_files = [i[26:34] for i in remote_files]
            remote_files.sort()
            remote_files = set(remote_files)
            log(
                f"Remote files identified when specified date was not found: {remote_files}"
            )
            skip = Skipped(f"No files where found for date {date}")
            raise ENDRUN(state=skip)
        log(f"Remote files identified: {remote_files}")
    else:
        _, stdout, _ = ssh_client.exec_command(
            f"find {vols_remote_directory} -name '{startswith}*.vol'"
        )
        all_files = stdout.read().decode("utf-8").splitlines()
        remote_files = [
            file
            for file in all_files
            if file.split("/")[-1][: len(greater_than) + 7]
            > f"{startswith}{greater_than}"
        ]
        log(f"Remote files identified: {remote_files}")
        if get_only_last_file:
            remote_files.sort()
            remote_files = [remote_files[-1]]
            log(f"Last remote file: {remote_files}")

    # Stop flow if there is no new file
    if len(remote_files) == 0:
        skip = Skipped("No new available files")
        raise ENDRUN(state=skip)

    # Filter files with same filename
    filenames = set()
    filtered_remote_files = []
    for file in remote_files:
        filename = file.split("/")[-1]
        log(f"filename split: {filename}")
        if filename not in filenames:
            filtered_remote_files.append(file)
            filenames.add(filename)
    remote_files = filtered_remote_files

    log(f"Found {len(remote_files)} files.")
    log(f"Remote files: {remote_files}")
    return remote_files, output_directory_path


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=30),
)
def fetch_vol_file(
    remote_file: str,
    radar: str,
    output_directory: str = "/var/escritoriodedados/temp/",
):
    """
    Fetch files from INEA server

    Args:
        remote_file (str): Remote file to be fetched
        radar (str): Radar name. Must be `gua` or `mac`
        output_directory (str): Directory where the files will be saved
    """

    # Create vars based on radar name
    if radar == "gua":
        env_variable = "INEA_SSH_PASSWORD"
        hostname = "a9921"
    elif radar == "mac":
        env_variable = "INEA_MAC_SSH_PASSWORD"
        hostname = "a9915"

    # APAGAR LOG
    log(f"Radar: {radar} env {env_variable}")
    # Get SSH password from env
    ssh_password = getenv(env_variable)

    # Open SSH client
    ssh_client = SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(
        hostname=hostname,
        username="root",
        password=ssh_password,
        timeout=300,
        auth_timeout=300,
        banner_timeout=300,
    )

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
    output_format: str = "HDF5",
    convert_params: str = "-k=ODIM2.1 -M=All",
) -> List[str]:
    """
    Convert VOL files to NetCDF using the `volconvert` CLI tool.
    For output_format = "NetCDF" convert_params must be
      "-f=Whole -k=CFext -r=Short -p=Radar -M=All -z"
    For output_format = "HDF5" convert_params must be "-k=ODIM2.1 -M=All" for all products
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
    # pylint: disable=consider-using-with
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
