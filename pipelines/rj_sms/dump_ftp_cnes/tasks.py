# -*- coding: utf-8 -*-

import os
import shutil
from datetime import datetime
import tempfile
import re
import pandas as pd
import pytz
from prefect import task
from pipelines.utils.utils import log
from pipelines.rj_sms.dump_ftp_cnes.constants import constants
from pipelines.rj_sms.utils import (
    list_files_ftp,
    upload_to_datalake)


@task
def check_newest_file_version(
    host: str,
    user: str,
    password: str,
    directory: str,
    file_name: str
):
    """
    Check the newest version of a file in a given FTP directory.

    Args:
        host (str): FTP server hostname.
        user (str): FTP server username.
        password (str): FTP server password.
        directory (str): FTP directory path.
        file_name (str): Base name of the file to check.

    Returns:
        str: The name of the newest version of the file.
    """
    file_name = constants.BASE_FILE.value
    files = list_files_ftp.run(host, user, password, directory)

    # filter a list of files that contains the base file name
    files = [file for file in files if file_name in file]

    # sort list descending
    files.sort(reverse=True)
    newest_file = files[0]

    # extract snapshot date from file
    snapshot_date = re.findall(r"\d{6}", newest_file)[0]
    snapshot_date = f"{snapshot_date[:4]}-{snapshot_date[-2:]}-01"

    log(f"Newest file: {newest_file}, snapshot_date: {snapshot_date}")
    return {"file": newest_file, "snapshot": snapshot_date}


@task
def conform_csv_to_gcp(directory: str):
    """
    Conform CSV files in the given directory to be compatible with Google Cloud Storage.

    Args:
        directory (str): The directory containing the CSV files to be conformed.

    Returns:
        List[str]: A list of filepaths of the conformed CSV files.
    """
    # list all csv files in the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    log(f"Conforming {len(csv_files)} files...")

    files_conform = []

    # iterate over each csv file
    for csv_file in csv_files:
        # construct the full file path
        filepath = os.path.join(directory, csv_file)

        # create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tf:
            # open the original file in read mode
            with open(filepath, 'r', encoding='iso8859-1') as f:
                # read the first line
                first_line = f.readline()

                # modify the first line
                modified_first_line = first_line.replace("TO_CHAR(", "")
                modified_first_line = modified_first_line.replace(",'DD/MM/YYYY')", "")

                # write the modified first line to the temporary file
                tf.write(modified_first_line)

                # copy the rest of the lines from the original file to the temporary file
                shutil.copyfileobj(f, tf)

        # replace the original file with the temporary file
        shutil.move(tf.name, filepath)
        files_conform.append(filepath)

    log("Conform done.")

    return files_conform


@task
def upload_multiple_tables_to_datalake(
    path_files: str,
    dataset_id: str,
    dump_mode: str
):
    """
    Uploads multiple tables to datalake.

    Args:
        path_files (str): The path to the files to be uploaded.
        dataset_id (str): The ID of the dataset to upload the files to.
        dump_mode (str): The dump mode to use for the upload.

    Returns:
        None
    """
    for n, file in enumerate(path_files):

        log(f"Uploading {n+1}/{len(path_files)} files to datalake...")

        # retrieve file name from path
        file_name = os.path.basename(file)

        # replace 6 digits numbers from string
        table_id = re.sub(r"\d{6}", "", file_name)
        table_id = table_id.replace(".csv", "")

        upload_to_datalake.run(
            input_path=file,
            dataset_id=dataset_id,
            table_id=table_id,
            if_exists="replace",
            csv_delimiter=";",
            if_storage_data_exists="replace",
            biglake_table=True,
            dump_mode=dump_mode
        )


@task
def add_multiple_date_column(
    directory: str,
    sep=";",
    snapshot_date=None
):
    """
    Adds date metadata columns to all CSV files in a given directory.

    Args:
        directory (str): The directory containing the CSV files.
        sep (str, optional): The delimiter used in the CSV files. Defaults to ";".
        snapshot_date (str, optional): The date of the snapshot. Defaults to None.
    """
    tz = pytz.timezone("Brazil/East")
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    # list all csv files in the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    # iterate over each csv file
    for n, csv_file in enumerate(csv_files):

        log(f"Adding date metadata to {n+1}/{len(csv_files)} files ...")
        # construct the full file path

        filepath = os.path.join(directory, csv_file)

        df = pd.read_csv(filepath, sep=sep, keep_default_na=False, dtype="str")
        df["_data_carga"] = now
        df["_data_snapshot"] = snapshot_date

        df.to_csv(filepath, index=False, sep=sep, encoding="utf-8")
        log(f"Column added to {filepath}")
