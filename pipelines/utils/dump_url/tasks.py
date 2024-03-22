# -*- coding: utf-8 -*-
# pylint: disable=E1101
"""
General purpose tasks for dumping data from URLs.
"""

from datetime import datetime, timedelta
import io
from pathlib import Path
from typing import List

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import gspread
import pandas as pd
from prefect import task
import requests

from pipelines.constants import constants
from pipelines.utils.utils import (
    remove_columns_accents,
)
from pipelines.utils.dump_url.utils import handle_dataframe_chunk
from pipelines.utils.utils import (
    get_credentials_from_env,
    log,
)


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
# pylint: disable=R0912,R0914,R0915
def download_url(  # pylint: disable=too-many-arguments
    url: str,
    fname: str,
    url_type: str = "direct",
    gsheets_sheet_order: int = 0,
    gsheets_sheet_name: str = None,
    gsheets_sheet_range: str = None,
) -> None:
    """
    Downloads a file from a URL and saves it to a local file.
    Try to do it without using lots of RAM.
    It is not optimized for Google Sheets downloads.

    Args:
        url: URL to download from.
        fname: Name of the file to save to.
        url_type: Type or URL that is being passed.
            `direct`-> common URL to download directly;
            `google_drive`-> Google Drive URL;
            `google_sheet`-> Google Sheet URL.
        gsheets_sheet_order: Worksheet index, in the case you want to select it by index. \
            Worksheet indexes start from zero.
        gsheets_sheet_name: Worksheet name, in the case you want to select it by name.
        gsheets_sheet_range: Range in selected worksheet to get data from. Defaults to entire \
            worksheet.

    Returns:
        None.
    """
    filepath = Path(fname)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    if url_type == "google_sheet":
        url_prefix = "https://docs.google.com/spreadsheets/d/"
        if not url.startswith(url_prefix):
            raise ValueError(
                "URL must start with https://docs.google.com/spreadsheets/d/"
                f"Invalid URL: {url}"
            )
        log(">>>>> URL is a Google Sheets URL, downloading directly")
        credentials = get_credentials_from_env(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
        )
        gspread_client = gspread.authorize(credentials)
        sheet = gspread_client.open_by_url(url)
        if gsheets_sheet_name:
            worksheet = sheet.worksheet(gsheets_sheet_name)
        else:
            worksheet = sheet.get_worksheet(gsheets_sheet_order)
        if gsheets_sheet_range:  # if range is informed, get range from worksheet
            dataframe = pd.DataFrame(worksheet.batch_get((gsheets_sheet_range,))[0])
        else:
            dataframe = pd.DataFrame(worksheet.get_values())
        new_header = dataframe.iloc[0]  # grab the first row for the header
        dataframe = dataframe[1:]  # take the data less the header row
        dataframe.columns = new_header  # set the header row as the df header
        log(f">>>>> Dataframe shape: {dataframe.shape}")
        log(f">>>>> Dataframe columns: {dataframe.columns}")
        dataframe.columns = remove_columns_accents(dataframe)
        log(f">>>>> Dataframe columns after treatment: {dataframe.columns}")
        dataframe.to_csv(filepath, index=False)
    elif url_type == "direct":
        log(">>>>> URL is not a Google Drive URL, downloading directly")
        req = requests.get(url, stream=True)
        with open(fname, "wb") as file:
            for chunk in req.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
                    file.flush()
    elif url_type == "google_drive":
        log(">>>>> URL is a Google Drive URL, downloading from Google Drive")
        # URL is in format
        # https://drive.google.com/file/d/<FILE_ID>/...
        # We want to extract the FILE_ID
        log(">>>>> Extracting FILE_ID from URL")
        url_prefix = "https://drive.google.com/file/d/"
        if not url.startswith(url_prefix):
            raise ValueError(
                "URL must start with https://drive.google.com/file/d/."
                f"Invalid URL: {url}"
            )
        file_id = url.removeprefix(url_prefix).split("/")[0]
        log(f">>>>> FILE_ID: {file_id}")
        creds = get_credentials_from_env(
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        try:
            service = build("drive", "v3", credentials=creds)
            request = service.files().get_media(fileId=file_id)  # pylint: disable=E1101
            fh = io.FileIO(fname, mode="wb")  # pylint: disable=C0103
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                log(f"Downloading file... {int(status.progress() * 100)}%.")
        except HttpError as error:
            log(f"HTTPError: {error}", "error")
            raise error
    else:
        raise ValueError("Invalid URL type. Please set values to `url_type` parameter")


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
# pylint: disable=R0913
def dump_files(
    file_path: str,
    partition_columns: List[str],
    save_path: str = ".",
    chunksize: int = 10**6,
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
    encoding: str = "utf-8",
    on_bad_lines: str = "error",
    separator: str = ",",
) -> None:
    """
    Dump files according to chunk size and read mode
    """
    event_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    for idx, chunk in enumerate(
        pd.read_csv(
            Path(file_path),
            chunksize=chunksize,
            encoding=encoding,
            on_bad_lines=on_bad_lines,
            sep=separator,
        )
    ):
        log(f"Dumping batch {idx} with size {chunksize}")
        handle_dataframe_chunk(
            dataframe=chunk,
            save_path=save_path,
            partition_columns=partition_columns,
            event_id=event_id,
            idx=idx,
            build_json_dataframe=build_json_dataframe,
            dataframe_key_column=dataframe_key_column,
        )
