# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_rdo
"""

from datetime import datetime
import re
import os
from pathlib import Path
from dateutil import parser


import pandas as pd

import pendulum
from prefect import task

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.constants import (
    constants as rdo_constants,
)
from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.utils import (
    build_table_id,
    merge_file_info_and_errors,
)

from pipelines.rj_smtr.utils import (
    connect_ftp,
    get_last_run_timestamp,
)
from pipelines.utils.utils import log, get_redis_client


@task
def get_file_paths_from_ftp(
    transport_mode: str, report_type: str, wait=None, dump=False
):  # pylint: disable=W0613
    """
    Search for files inside previous interval (days) from current date,
    get filename and partitions (from filename) on FTP client.
    """

    min_timestamp = datetime(2022, 1, 1).timestamp()  # set min timestamp for search
    # Connect to FTP & search files
    # try:
    ftp_client = connect_ftp(constants.RDO_FTPS_SECRET_PATH.value)
    files_updated_times = {
        file: datetime.timestamp(parser.parse(info["modify"]))
        for file, info in ftp_client.mlsd(transport_mode)
    }
    # Get files modified inside interval
    files = []
    for filename, file_mtime in files_updated_times.items():
        if file_mtime >= min_timestamp:
            if filename[:3] == report_type and "HISTORICO" not in filename:
                # Get date from file
                date = re.findall("2\\d{3}\\d{2}\\d{2}", filename)[-1]

                file_info = {
                    "transport_mode": transport_mode,
                    "report_type": report_type,
                    "filename": filename.split(".")[0],
                    "ftp_path": transport_mode + "/" + filename,
                    "partitions": f"ano={date[:4]}/mes={date[4:6]}/dia={date[6:]}",
                    "error": None,
                }
                # log(f"Create file info: {file_info}")
                files.append(file_info)
    # except Exception as e:  # pylint: disable=W0703
    #     return [{"error": e}]
    log(f"There are {len(files)} files at the FTP")
    return files


@task
def check_files_for_download(files: list, dataset_id: str, table_id: str):
    """Check redis for files already downloaded from the FTP

    Args:
        files (list): file informations gathered from FTP
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery

    Returns:
        list: Containing the info on the files to download
    """
    redis_client = get_redis_client()
    exclude_files = redis_client.get(f"{dataset_id}.{table_id}")["files"]
    log(f"There are {len(exclude_files)} already downloaded")
    download_files = [
        file_info for file_info in files if file_info["filename"] not in exclude_files
    ]
    log(f"Will download the remaining {len(download_files)} files:{download_files}")
    return download_files


@task
def download_and_save_local_from_ftp(file_info: dict):
    """
    Downloads file from FTP and saves to data/raw/<dataset_id>/<table_id>.
    """
    # table_id: str, kind: str, rho: bool = False, rdo: bool = True
    if file_info["error"] is not None:
        return file_info

    dataset_id = constants.RDO_DATASET_ID.value
    base_path = (
        f'{os.getcwd()}/{os.getenv("DATA_FOLDER", "data")}/{{bucket_mode}}/{dataset_id}'
    )

    table_id = build_table_id(  # mudar pra task
        mode=file_info["transport_mode"], report_type=file_info["report_type"]
    )

    # Set general local path to save file (bucket_modes: raw or staging)
    file_info[
        "local_path"
    ] = f"""{base_path}/{table_id}/{file_info["partitions"]}/{file_info['filename']}.{{file_ext}}"""
    # Get raw data
    file_info["raw_path"] = file_info["local_path"].format(
        bucket_mode="raw", file_ext="txt"
    )
    Path(file_info["raw_path"]).parent.mkdir(parents=True, exist_ok=True)
    try:
        # Get data from FTP - TODO: create get_raw() error alike
        ftp_client = connect_ftp(constants.RDO_FTPS_SECRET_PATH.value)
        if not Path(file_info["raw_path"]).is_file():
            with open(file_info["raw_path"], "wb") as raw_file:
                ftp_client.retrbinary(
                    "RETR " + file_info["ftp_path"],
                    raw_file.write,
                )
        ftp_client.quit()
        # Get timestamp of download time
        file_info["timestamp_captura"] = pendulum.now(
            constants.TIMEZONE.value
        ).isoformat()

        log(f"Timestamp captura is {file_info['timestamp_captura']}")
        log(f"Update file info: {file_info}")
    except Exception as error:  # pylint: disable=W0703
        file_info["error"] = error
    return file_info


@task(nout=4)
def pre_treatment_br_rj_riodejaneiro_rdo(
    files: list,
    divide_columns_by: int = 100,
) -> tuple:
    """Adds header, capture_time and standardize columns

    Args:
        file_info (dict): information for the files found in the current run
        divide_columns_by (int, optional): value which to divide numeric columns.
        Defaults to 100.

    Returns:
        dict: updated file_info with treated filepath
    """
    treated_paths, raw_paths, partitions, status = [], [], [], []
    log(f"Received {len(files)} to treat")
    for file_info in files:
        log(f"Processing file {files.index(file_info)}")
        try:
            config = rdo_constants.RDO_PRE_TREATMENT_CONFIG.value[
                file_info["transport_mode"]
            ][file_info["report_type"]]
            # context.log.info(f"Config for ETL: {config}")
            # Load data
            df = pd.read_csv(  # pylint: disable=C0103
                file_info["raw_path"], header=None, delimiter=";", index_col=False
            )  # pylint: disable=C0103
            if len(df) == 0:
                log("Dataframe is empty")
                status.append({"error": "ValueError: file is empty"})
                continue  # If file is empty, skip current iteration.
            log(f"Load csv from raw file:\n{df.head(5)}")
            # Set column names for those already in the file
            df.columns = config["reindex_columns"][: len(df.columns)]
            log(f"Found reindex columns at config:\n{df.head(5)}")
            # Treat column "codigo", add empty column if doesn't exist
            if ("codigo" in df.columns) and (file_info["transport_mode"] == "STPL"):
                df["codigo"] = df["codigo"].str.extract("(?:VAN)(\\d+)").astype(str)
            else:
                df["codigo"] = ""
            # Order columns
            # if config["reorder_columns"]:
            #     ordered = [
            #         config["reorder_columns"][col]
            #         if col in config["reorder_columns"].keys()
            #         else i
            #         for i, col in enumerate(config["reindex_columns"])
            #     ]
            #     df = df[list(config["reindex_columns"][col] for col in ordered)]
            # # pylint: disable=C0103
            # else:
            df = df[config["reindex_columns"]]
            # Add timestamp column
            df["timestamp_captura"] = file_info["timestamp_captura"]
            log(f"Added timestamp_captura: {file_info['timestamp_captura']}")
            log(f"Before dividing df is\n{df.head(5)}")
            # Divide columns by value
            if config["divide_columns"]:
                df[config["divide_columns"]] = df[config["divide_columns"]].apply(
                    lambda x: x / divide_columns_by, axis=1
                )
            # Save treated data
            file_info["treated_path"] = file_info["local_path"].format(
                bucket_mode="staging", file_ext="csv"
            )
            Path(file_info["treated_path"]).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(file_info["treated_path"], index=False)
            log(f'Saved treated data to: {file_info["treated_path"]}')
            log(f"Updated file info is:\n{file_info}")
            # Build returns
            treated_paths.append(file_info["treated_path"])
            raw_paths.append(file_info["raw_path"])
            partitions.append(file_info["partitions"])
            status.append({"error": None})
        except Exception as e:  # pylint: disable=W0703
            log(f"Pre Treatment failed with error: {e}")
            treated_paths.append(None)
            raw_paths.append(None)
            partitions.append(None)
            status.append({"error": e})
    return treated_paths, raw_paths, partitions, status


@task
def update_rdo_redis(
    download_files: list,
    table_id: str,
    dataset_id: str = constants.RDO_DATASET_ID.value,
    errors=None,
    wait=None,  # pylint: disable=W0613
):
    """
    Update files downloaded to redis, if uploaded correctly.

    Args:
        download_files (list): information on the downloaded files
        table_id (str): table_id on BigQuery
        dataset_id (str, optional): dataset_id on BigQuery.
        Defaults to constants.RDO_DATASET_ID.value.
        errors (list, optional): list of errors. Defaults to None.
        wait (Any, optional): wait for task before run. Defaults to None.

    Returns:
        bool: if redis key was set
    """
    key = f"{dataset_id}.{table_id}"
    redis_client = get_redis_client()
    content = redis_client.get(key)  # get current redis state
    if errors:
        log(f"Received errors:\n {errors}")
        merge_file_info_and_errors(download_files, errors)
    log(f"content is:\n{content['files'][:5]}")
    insert_content = [
        file_info["filename"] for file_info in download_files if not file_info["error"]
    ]  # parse filenames to append
    log(
        f"""
    Will register {len(insert_content)} files:
        {insert_content}
    """
    )
    content["files"].extend(insert_content)  # generate updated dict to set
    log(f"After appending, content has {len(content['files'])} files registered")
    return redis_client.set(key, content)


@task
def get_rdo_date_range(dataset_id: str, table_id: str, mode: str = "prod"):
    """Get date range for RDO/RHO materialization

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        mode (str, optional): mode to materialize to.
        Accepted options are 'dev' and 'prod'. Defaults to "prod".

    Returns:
        dict: containing 'date_range_start' and 'date_range_end' keys
    """
    last_run_date = get_last_run_timestamp(
        dataset_id=dataset_id, table_id=table_id, mode=mode
    )
    if not last_run_date:
        last_run_date = constants.RDO_MATERIALIZE_START_DATE.value
    return {
        "date_range_start": last_run_date,
        "date_range_end": pendulum.now(constants.TIMEZONE.value).date().isoformat(),
    }
