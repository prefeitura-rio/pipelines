# -*- coding: utf-8 -*-
"""
Tasks for gtfs
"""

from prefect import task
from typing import Dict
from pathlib import Path
import zipfile
import pandas as pd

from pipelines.rj_smtr.constants import constants

from pipelines.utils.utils import (
    get_storage_blobs,
    log,
)


@task
def download_gtfs(
    dataset_id: str = constants.GTFS_DATASET_ID.value,
    feed_start_date: str = None,
    feed_end_date: str = None,
) -> Dict:
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
            log(f"Downloaded file {filename}")

            # Extract ZIP file
            with zipfile.ZipFile(filename, "r") as zip_ref:
                zip_ref.extractall(dirpath)

    mapped_tables_status = {
        "table_id": [],
        "status": []
    }

    for table_id in constants.GTFS_TABLES.value:

        try:
            data = pd.read_csv(f"gtfs/{table_id}.txt")
            # Corrige data de inicio e final do feed
            if table_id == "feed_info":
                if feed_start_date:
                    data["feed_start_date"] = feed_start_date
                if feed_end_date:
                    data["feed_end_date"] = feed_end_date
        # TODO: Adicionar catch + log de erro
        except:
            error = None
            pass

        mapped_tables_status["table_id"].append(table_id)
        mapped_tables_status["status"].append({"data": data, "error": error})

    return mapped_tables_status