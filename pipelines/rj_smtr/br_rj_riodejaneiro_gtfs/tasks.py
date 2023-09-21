# -*- coding: utf-8 -*-
"""
Tasks for gtfs
"""

import traceback
import zipfile
from pathlib import Path
from datetime import datetime
import pandas as pd
from prefect import task
from pipelines.rj_smtr.constants import constants
from pipelines.utils.utils import get_storage_blobs, log

from pipelines.rj_smtr.tasks import get_current_timestamp


@task
def get_current_timestamp_from_date(
    date: str,
) -> datetime:
    """
    Get current timestamp from date
    Args:
        date (str): Date in format YYYY-MM-DD
    Returns:
        datetime: Current timestamp
    """

    return get_current_timestamp.run(datetime.strptime(date, "%Y-%m-%d"))


@task
def download_gtfs(
    dataset_id: str = constants.GTFS_DATASET_ID.value,
    feed_start_date: str = None,
    feed_end_date: str = None,
) -> str:
    """
    Retrieve GTFS data from GCS and saves locally without partitioning.
    Args:
        dataset_id (str, optional): Dataset ID on GCS/BigQuery
        feed_start_date (str):
        feed_end_date (str):
    Returns:
        list: Containing dicts with the following keys for each gtfs table:
          * `table_id`: Table ID
          * `filepath`: Filepath of the treated data
          * `data`: Data
          * `error`: error catched from data retrieval
    """

    error = None

    # Download gtfs.zip + quadro.csv
    dirpath = Path(constants.GTFS_RAW_PATH.value)
    dirpath.mkdir(parents=True, exist_ok=True)

    log(f"Path criado: {dirpath.as_posix()}")

    blobs = get_storage_blobs(
        dataset_id=dataset_id, table_id="upload", mode="development"
    )

    log(f"Retrieved blobs: {blobs}")

    for blob in blobs:
        filename = blob.name.split("/")[-1]

        # Download arquivo de quadro horário
        if filename == "quadro.csv":
            blob.download_to_filename(
                f"{dirpath.as_posix()}/{filename.split('.')[0]}.txt"
            )
            log(f"File {filename} downloaded to {dirpath.as_posix()}/{filename}")

        # Download e extrai arquivos GTFS
        elif filename == "gtfs.zip":
            # Download ZIP file
            blob.download_to_filename(filename)

            # Extract ZIP file
            with zipfile.ZipFile(filename, "r") as zip_ref:
                zip_ref.extractall(dirpath)

    mapped_tables_status = {"table_id": [], "status": []}

    # mudar para json a saída de dados e aplicar o raw path
    for table_id in constants.GTFS_TABLES.value:
        try:
            data = pd.read_csv(
                dirpath.as_posix() + "/" + str(table_id) + ".txt"
            ).to_dict(orient="records")

            # Corrige data de inicio e final do feed

            if table_id == "feed_info":
                if feed_start_date:
                    data["feed_start_date"] = feed_start_date
                if feed_end_date:
                    data["feed_end_date"] = feed_end_date

        # Adicionar catch + log de erro
        except Exception:
            error = traceback.format_exc()

            log(f"[CATCHED ERROR]: \n{error}")

        mapped_tables_status["table_id"].append(table_id)
        mapped_tables_status["status"].append({"data": data, "error": error})

    log(f"Table {list(mapped_tables_status)} mapped")

    return mapped_tables_status
