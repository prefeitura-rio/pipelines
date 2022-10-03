# -*- coding: utf-8 -*-
"""
Tasks for INEA.
"""
from os import environ
from pathlib import Path

from paramiko import SSHClient
from prefect import task
from scp import SCPClient

from pipelines.utils.utils import log


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
    # Open SSH client
    ssh_client = SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(hostname="a9921")
    # Open SCP client
    scp = SCPClient(ssh_client.get_transport())
    # Fetch VOL files
    fname = f"/var/opt/edge/vols/9921GUA{date}*.vol"
    scp.get(fname, recursive=True, local_path=output_directory)
    # Close connection
    scp.close()


@task
def convert_vol_files(output_directory: str = "/var/escritoriodedados/temp/"):
    """
    Convert VOL files to NetCDF using the `volconvert` CLI tool.
    """
    # List all files in the output directory
    output_directory_path = Path(output_directory)
    files = output_directory_path.glob("*.vol")
    # Log each file and then delete it
    for file in files:
        log(f"Converting {file} to NetCDF...")
        file.unlink()
