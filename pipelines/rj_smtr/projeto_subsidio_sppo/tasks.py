# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from prefect import task
from typing import Dict
from pathlib import Path
import zipfile
import os
import shutil

from pipelines.rj_smtr.constants import constants as smtr_constants

from pipelines.utils.utils import (
    get_storage_blobs,
    get_credentials_from_env,
    list_blobs_with_prefix,
    log,
)


def create_local_partition_path(
    dataset_id: str, table_id: str, filename: str, partitions: str = None
) -> str:
    """
    Create the full path sctructure which to save data locally before
    upload.

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        filename (str, optional): Single csv name
        partitions (str, optional): Partitioned directory structure, ie "ano=2022/mes=03/data=01"
    Returns:
        str: String path having `mode` and `filetype` to be replaced afterwards,
    either to save raw or staging files.
    """
    data_folder = os.getenv("DATA_FOLDER", "data")
    file_path = f"{os.getcwd()}/{data_folder}/{{mode}}/{dataset_id}/{table_id}"
    file_path += f"/{partitions}/{filename}.{{filetype}}"
    log(f"Creating file path: {file_path}")
    return file_path


@task
def get_raw_and_save_local(
    table_id: list,
    filename: str,
    partitions: list,  # ["staging/.../quadro/...", "staging/.../shapes/...", ]
    dataset_id: str = smtr_constants.SUBSIDIO_SPPO_PREPROD_DATASET_ID.value,
    mode: str = "raw",
) -> Dict:
    """
    Retrieve GTFS data from GCS
    """

    error = None

    # TODO: checa no redis a ultima versao capturada

    # Download gtfs.zip + quadro.csv
    blobs = get_storage_blobs(
        dataset_id=dataset_id, table_id="upload_gtfs", mode="development"
    )

    raw_filepath = []

    for blob in blobs:
        blob_name = blob.name.split("/")[-1]

        # Download single CSV file
        if blob_name == "quadro.csv":
            filepath = create_local_partition_path(
                dataset_id=dataset_id,
                table_id="quadro",
                filename=filename,
                partitions=partitions,
            )
            _file_path = filepath.format(mode=mode, filetype="csv")
            Path().parent.mkdir(parents=True, exist_ok=True)

            blob.download_to_filename(_file_path)
            raw_filepath.append(_file_path)
            log.info(f"File {blob_name} downloaded to {_file_path}")

        # Download, extract and save GTFS files from ZIP
        elif blob_name == "gtfs.zip":
            # Download ZIP file
            blob.download_to_filename(blob_name)  # filename = gtfs.zip
            # Extract ZIP file
            with zipfile.ZipFile(blob_name, "r") as zip_ref:
                zip_ref.extractall()

            # Save extracted files with partitions
            zip_folder = filename.split(".")[0]  # zip_folder = gtfs

            for table in os.listdir(zip_folder):
                if table in table_id:
                    filepath = create_local_partition_path(
                        dataset_id=dataset_id,
                        table_id=table_id,
                        filename=filename,
                        partitions=partitions,
                    )
                    _file_path = filepath.format(mode=mode, filetype="txt")
                    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)

                    table_path = f"{zip_folder}/{table}.txt"
                    shutil.move(table_path, _file_path)
                    raw_filepath.append(_file_path)
                    log.info(f"File {table}.txt downloaded to {_file_path}")

    return {"raw_filepath": raw_filepath, "error": error}

    # data = None
    # url = smtr_constants.SUBSIDIO_SPPO_GTFS_ENDPOINTS.value[table_id]

    # # Get data from API
    # try:
    #     response = requests.get(url, timeout=smtr_constants.MAX_TIMEOUT_SECONDS.value)
    #     error = None
    # except Exception as exp:
    #     error = exp
    #     log(f"[CATCHED] Task failed with error: \n{error}", level="error")
    #     return {"data": None, "error": error}

    # # Check data results
    # if response.ok:  # status code is less than 400
    #     data = response.json()
    #     if isinstance(data, dict) and "DescricaoErro" in data.keys():
    #         error = data["DescricaoErro"]
    #         log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    # return {"data": data, "error": error, "date_modifed": "2021-08-31"}


@task
def pre_treatment_subsidio_sppo_gtfs(status: Dict, timestamp: str) -> Dict:
    """
    ...
    """

    return status
