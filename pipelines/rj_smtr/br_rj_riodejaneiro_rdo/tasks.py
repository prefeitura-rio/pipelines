# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_rdo
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################
from datetime import datetime, timedelta
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
from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.utils import build_table_id
from pipelines.rj_smtr.utils import connect_ftp, get_last_run_timestamp
from pipelines.utils.utils import log


@task
def get_file_paths_from_ftp(
    transport_mode: str, report_type: str, wait=None, dump=False
):  # pylint: disable=W0613
    """
    Search for files inside previous interval (days) from current date,
    get filename and partitions (from filename) on FTP client.
    """

    execution_time = pendulum.now(constants.TIMEZONE.value)
    # Define interval for date search
    now = execution_time + timedelta(
        hours=11, minutes=30
    )  # Sunday, January 9, 2022 2:30:00 PM
    max_timestamp = now.timestamp()
    if not dump:
        min_timestamp = (
            now - timedelta(days=1)
        ).timestamp()  # Saturday, January 8, 2022 2:30:00 PM
        log(f"{execution_time} of type {type(execution_time)}")
    else:
        min_timestamp = datetime(2022, 1, 1).timestamp()
    # Connect to FTP & search files
    ftp_client = connect_ftp()
    files_updated_times = {
        file: datetime.timestamp(parser.parse(info["modify"]))
        for file, info in ftp_client.mlsd(transport_mode)
    }
    # Get files modified inside interval
    files = []
    for filename, file_mtime in files_updated_times.items():
        if min_timestamp <= file_mtime < max_timestamp:
            if filename[:3] == report_type and "HISTORICO" not in filename:
                log(
                    f"""
                    Found file
                    - {filename}
                    at folder
                    - {transport_mode}
                    with timestamp
                    - {str(file_mtime)}"""
                )
                # Get date from file
                date = re.findall("2\\d{3}\\d{2}\\d{2}", filename)[-1]

                file_info = {
                    "transport_mode": transport_mode,
                    "report_type": report_type,
                    "filename": filename.split(".")[0],
                    "ftp_path": transport_mode + "/" + filename,
                    "partitions": f"ano={date[:4]}/mes={date[4:6]}/dia={date[6:]}",
                }
                log(f"Create file info: {file_info}")
                files.append(file_info)
    return files


@task
def download_and_save_local_from_ftp(file_info: dict):
    """
    Downloads file from FTP and saves to data/raw/<dataset_id>/<table_id>.
    """
    # table_id: str, kind: str, rho: bool = False, rdo: bool = True
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
    # Get data from FTP - TODO: create get_raw() error alike
    ftp_client = connect_ftp()
    if not Path(file_info["raw_path"]).is_file():
        with open(file_info["raw_path"], "wb") as raw_file:
            ftp_client.retrbinary(
                "RETR " + file_info["ftp_path"],
                raw_file.write,
            )
    ftp_client.quit()
    # Get timestamp of download time
    file_info["timestamp_captura"] = pendulum.now(constants.TIMEZONE.value).isoformat()

    log(f"Timestamp captura is {file_info['timestamp_captura']}")
    log(f"Update file info: {file_info}")
    return file_info


@task(nout=3)
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
    treated_paths, raw_paths, partitions = [], [], []
    for file_info in files:
        try:
            config = rdo_constants.RDO_PRE_TREATMENT_CONFIG.value[
                file_info["transport_mode"]
            ][file_info["report_type"]]
            # context.log.info(f"Config for ETL: {config}")
            # Load data
            df = pd.read_csv(  # pylint: disable=C0103
                file_info["raw_path"], header=None, delimiter=";", index_col=False
            )  # pylint: disable=C0103
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
        except Exception as e:  # pylint: disable=W0703
            log(f"Pre Treatment failed with error: {e}")
            continue
    return treated_paths, raw_paths, partitions


@task
def get_rdo_date_range(dataset_id: str, table_id: str, mode: str = "prod"):
    last_run_date = get_last_run_timestamp(dataset_id=dataset_id, table_id=table_id)
    if not last_run_date:
        last_run_date = constants.RDO_MATERIALIZE_START_DATE.value
    return {
        "date_range_start": last_run_date,
        "date_range_end": pendulum.now(constants.TIMEZONE.value).date().isoformat(),
    }
