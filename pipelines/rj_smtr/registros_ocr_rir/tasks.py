# -*- coding: utf-8 -*-
"""
Tasks for monitoramento_RIR
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
from pathlib import Path
import pandas as pd
import pendulum
from prefect import task
from pipelines.rj_smtr.utils import connect_ftp
from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.registros_ocr_rir.utils import log_error
from pipelines.utils.utils import log


@task
def get_files_from_ftp(dump: bool = False, execution_time: str = None):
    """Search FTP for files created in the same minute as the
    capture time.

    Args:
        dump (bool, optional): if True will dump all files found on the FTP.
        Defaults to False.
        execution_time (str, optional): optionally, search for a file created
        at a given minute. Defaults to None.

    Returns:
        dict: 'capture' is a flag for skipping tasks if no files were found,
        'file_info' is the info for processing the captured file
    """
    if execution_time:
        execution_time = datetime.fromisoformat(execution_time)
    else:
        execution_time = pendulum.now(constants.TIMEZONE.value).replace(
            tzinfo=None, second=0, microsecond=0
        )
    start_date = datetime.fromisoformat(constants.RIR_START_DATE.value)
    file_info = []
    try:
        ftp_client = connect_ftp(
            secret_path=constants.RIR_SECRET_PATH.value, secure=False
        )
        files = {
            file: datetime.strptime(
                file.split(".")[0].split("_")[1], "%Y%m%d%H%M%S"
            ).replace(second=0, microsecond=0)
            for file, info in ftp_client.mlsd()
            if file.startswith("ocrs")
        }
        if not dump:
            for file, created_time in files.items():
                if execution_time == created_time:
                    file_info.append(
                        {
                            "filename": file,
                            "created_time": created_time,
                            "partitions": f"data={created_time.day}/hora={created_time.hour}",
                        }
                    )
        else:
            file_info = [
                {
                    "filename": file,
                    "created_time": created_time,
                    "partitions": f"data={created_time.day}/hora={created_time.hour}",
                }
                for file, created_time in files.items()
                if created_time >= start_date
            ]
        log(f"Found {len(file_info)} files:\n{file_info}")
    except Exception as err:  # pylint: disable=W0703
        log_error(err)
    # add flag para skipar proximas tasks se não tiver arquivo
    return {"capture": bool(file_info), "file_info": file_info}


@task
def download_and_save_local(file_info: list):
    """Download files from FTP

    Args:
        file_info (list): containing dicts representing each file
        found.

    Returns:
        dict: updated file info with the local path for the downloaded
        file.
    """
    dataset_id = constants.RIR_DATASET_ID.value
    table_id = constants.RIR_TABLE_ID.value
    ftp_client = connect_ftp(secret_path=constants.RIR_SECRET_PATH.value, secure=False)
    for info in file_info:
        filepath = f"{dataset_id}/{table_id}/{info['partitions']}/{info['filename']}"
        Path(filepath).parent.mkdir(exist_ok=True, parents=True)
        with open(filepath, "wb") as file:
            ftp_client.retrbinary(
                "RETR " + info["filename"],
                file.write,
            )
        info["filepath"] = filepath
        log(f"Updated file info:\n{info}")
    ftp_client.quit()

    return file_info


@task
def pre_treatment_ocr(file_info: list):
    """Standardize columns

    Args:
        file_info (list): containing dicts representing each file
        found.

    Returns:
        str: path to the table folder containing partitioned files
    """
    primary_cols = constants.RIR_OCR_PRIMARY_COLUMNS.value
    secondary_cols = constants.RIR_OCR_SECONDARY_COLUMNS.value
    standard_cols = dict(primary_cols, **secondary_cols)
    log(f"Standard columns are:{standard_cols}")
    for info in file_info:
        log(f'open file {info["filepath"]}')
        data = pd.read_csv(info["filepath"], sep=";")
        log(
            f"""
        Received data:
        {data[:50]}
        with columns:
        {data.columns.to_list()}
            """
        )
        data["datahora"] = pd.to_datetime(data["DATA"] + " " + data["HORA"])
        log(f"Created column datahora as:\n{data['datahora']}")
        for col, new_col in secondary_cols.items():
            if col not in data.columns:
                log(f"Add empty column {col} to data")
                data[new_col] = ""
        data = data.rename(columns=standard_cols)[list(standard_cols.values())]
        data.to_csv(info["filepath"], index=False)
    table_dir = f"{constants.RIR_DATASET_ID.value}/{constants.RIR_TABLE_ID.value}"

    return table_dir
