# -*- coding: utf-8 -*-
"""
Tasks for rj_smtr
"""
# pylint: disable=W0703

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
import json
import os
from pathlib import Path
from datetime import timedelta

from basedosdados import Storage
from dbt_client import DbtClient
import pandas as pd
import pendulum
from prefect import task
import requests

from pipelines.rj_smtr.constants import constants
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.utils import (
    create_or_append_table,
    bq_project,
    get_table_max_value,
)
from pipelines.utils.execute_dbt_model.utils import get_dbt_client
from pipelines.utils.utils import log, get_vault_secret

###############
#
# DBT
#
###############


@task
def get_local_dbt_client(host: str, port: int):
    """Set a DBT client for running CLI commands. Requires
    building container image for your queries repository.

    Args:
        host (str): hostname. When running locally, usually 'localhost'
        port (int): the port number in which the DBT rpc is running

    Returns:
        DbtClient: object used to run DBT commands.
    """
    return get_dbt_client(host=host, port=port)


@task(
    checkpoint=False,
)
def run_dbt_command(  # pylint disable=R0913
    dbt_client: DbtClient,
    dataset_id: str = None,
    table_id: str = None,
    command: str = "run",
    flags: str = None,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Runs a dbt command. If passing a dataset_id only, will run the entire dataset.
    If also passing a table_id, will select the dbt model specified at the path:
    models/<dataset_id>/<table_id>.sql.

    Args:
        dbt_client (DbtClient): Dbt interface of interaction
        dataset_id (str, optional): dataset_id on BigQuery, also folder name on
        your queries repo. Defaults to None.
        table_id (str, optional): table_id on BigQuery, also .sql file name on your
        models folder. Defaults to None.
        command (str, optional): dbt command to run. Defaults to "run".
        flags (str, optional): flags allowed to the specific command.
        Should be preceeded by "--" Defaults to None.
        sync (bool, optional): _description_. Defaults to True.
    """
    run_command = f"dbt {command}"
    if dataset_id:
        run_command += f" --select models/{dataset_id}/"
        if table_id:
            run_command += f"{table_id}.sql"
    if flags:
        run_command += f" {flags}"

    log(f"Will run the following command:\n{run_command}")
    dbt_client.cli(run_command, sync=True)
    return log("Finished running dbt command")


@task(
    checkpoint=False,
    max_retries=emd_constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=emd_constants.TASK_RETRY_DELAY.value),
)
def run_dbt_schema(
    dbt_client: DbtClient,
    dataset_id: str,
    refresh: bool = False,
    wait=None,  # pylint: disable=unused-argument
):
    """Run a whole schema (dataset) worth of models

    Args:
        dbt_client (DbtClient): Dbt interface of interaction
        dataset_id (str, optional): Dataset id on BigQuery. Defaults to "br_rj_riodejaneiro_sigmob".
        refresh (bool, optional): If true, rebuild all models from scratch. Defaults to False.

    Returns:
        None
    """

    run_command = f"run --select models/{dataset_id}"
    if refresh:
        log(f"Will run the following command:\n{run_command} in full refresh mode")
        run_command += " --full-refresh"
    dbt_client.cli(run_command, sync=True)
    return log(f"Finished running schema {dataset_id}")


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def build_incremental_model(  # pylint: disable=too-many-arguments
    dbt_client: DbtClient,
    dataset_id: str,
    base_table_id: str,
    mat_table_id: str,
    field_name: str = "data_versao",
    refresh: bool = False,
    wait=None,  # pylint: disable=unused-argument
):
    """
        Utility task for backfilling table in predetermined steps.
        Assumes the step sizes will be defined on the .sql file.

    Args:
        dbt_client (DbtClient): DBT interface object
        dataset_id (str): Dataset id on BigQuery
        base_table_id (str): Base table from which to materialize (usually, an external table)
        mat_table_id (str): Target table id for materialization
        field_name (str, optional): Key field (column) for dbt incremental filters.
        Defaults to "data_versao".
        refresh (bool, optional): If True, rebuild the table from scratch. Defaults to False.
        wait (NoneType, optional): Placeholder parameter, used to wait previous tasks finish.
        Defaults to None.

    Returns:
        bool: whether the table was fully built or not.
    """

    query_project_id = bq_project()
    last_mat_date = get_table_max_value(
        query_project_id, dataset_id, mat_table_id, field_name
    )
    last_base_date = get_table_max_value(
        query_project_id, dataset_id, base_table_id, field_name
    )
    log(
        f"""
    Base table last version: {last_base_date}
    Materialized table last version: {last_mat_date}
    """
    )
    run_command = f"run --select models/{dataset_id}/{mat_table_id}.sql"

    if refresh:
        log("Running in full refresh mode")
        log(f"DBT will run the following command:\n{run_command+' --full-refresh'}")
        dbt_client.cli(run_command + " --full-refresh", sync=True)
        last_mat_date = get_table_max_value(
            query_project_id, dataset_id, mat_table_id, field_name
        )

    if last_base_date > last_mat_date:
        log("Running interval step materialization")
        log(f"DBT will run the following command:\n{run_command}")
        while last_base_date > last_mat_date:
            running = dbt_client.cli(run_command, sync=True)
            last_mat_date = get_table_max_value(
                query_project_id,
                dataset_id,
                mat_table_id,
                field_name,
                wait=running,
            )
            log(f"After this step, materialized table last version is: {last_mat_date}")
            if last_mat_date == last_base_date:
                log("Materialized table reached base table version!")
                return True
    log("Did not run interval step materialization...")
    return False


###############
#
# Local file managment
#
###############


@task
def create_current_date_hour_partition():
    """Create partitioned directory structure to save data locally based
    on capture time.

    Returns:
        dict: "filename" contains the name which to upload the csv, "partitions" contains
        the partitioned directory path
    """
    timezone = constants.TIMEZONE.value

    capture_time = pendulum.now(timezone)
    date = capture_time.strftime("%Y-%m-%d")
    hour = capture_time.strftime("%H")

    return {
        "filename": capture_time.strftime("%Y-%m-%d-%H-%M-%S"),
        "partitions": f"data={date}/hora={hour}",
    }


@task
def create_local_partition_path(dataset_id, table_id, filename, partitions):
    """Get the full path which to save data locally before upload.

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        filename (str): Single csv name
        partitions (str): Partitioned directory structure, ie "ano=2022/mes=03/data=01"

    Returns:
        str: Final path which to save files
    """

    # If not specific table_id, use resource one
    # if not table_id:
    #     table_id = context.resources.basedosdados_config["table_id"]
    # dataset_id = context.resources.basedosdados_config["dataset_id"]

    # Get data folder from environment variable
    data_folder = os.getenv("DATA_FOLDER", "data")

    file_path = f"{os.getcwd()}/{data_folder}/{{mode}}/{dataset_id}/{table_id}"
    file_path += f"/{partitions}/{filename}.{{filetype}}"
    log(f"Creating file path: {file_path}")

    return file_path


@task
def save_raw_local(data, file_path, mode="raw"):
    """Dumps json response from API to .json file

    Args:
        data (response): Response from API request
        file_path (str): Path which to save raw file
        mode (str, optional): Folder to save locally, later folder which to upload to GCS.
        Defaults to "raw".

    Returns:
        str: Path to the saved file
    """

    _file_path = file_path.format(mode=mode, filetype="json")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    json.dump(data.json(), Path(_file_path).open("w", encoding="utf-8"))

    return _file_path


@task
def save_treated_local(dataframe, file_path, mode="staging"):
    """Save treated file locally

    Args:
        dataframe (pandas.core.DataFrame): Data to save as .csv file
        file_path (_type_): Path which to save .csv files
        mode (str, optional): Directory to save locally, later folder which to upload to GCS.
        Defaults to "staging".

    Returns:
        str: Path to the saved file
    """

    _file_path = file_path.format(mode=mode, filetype="csv")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(_file_path, index=False)

    return _file_path


###############
#
# Extract data
#
###############


@task
def get_raw(url, headers=None, kind: str = None):
    """Request data from a url API

    Args:
        url (str): URL to send request to
        headers (dict, optional): Aditional fields to send along the request. Defaults to None.
        kind (str, optional): Kind of API being captured.
        Possible values are 'stpl', 'brt' and 'sppo'
    Returns:
        dict: "data" contains the response object from the request, "timestamp" contains
        the run time timestamp, "error" catches errors that may occur during task execution.
    """
    if kind == "stpl":
        headers = get_vault_secret("stpl_api")["data"]
    data = None
    error = None
    timestamp = pendulum.now(constants.TIMEZONE.value)
    try:
        data = requests.get(
            url, headers=headers, timeout=constants.MAX_TIMEOUT_SECONDS.value
        )
    except requests.exceptions.ReadTimeout as err:
        error = err
    except Exception as err:
        error = f"Unknown exception while trying to fetch data from {url}: {err}"

    if data is None:
        if error is None:
            error = "Data from API is none!"

    if error:
        return {"data": data, "timestamp": timestamp.isoformat(), "error": error}
    if data.ok:
        return {
            "data": data,
            "error": error,
            "timestamp": timestamp.isoformat(),
        }
    # else
    error = f"Requests failed with error {data.status_code}"
    return {"error": error, "timestamp": timestamp.isoformat(), "data": data}


###############
#
# Load data
#
###############


@task
def bq_upload(dataset_id, table_id, filepath, raw_filepath=None, partitions=None):
    """Upload raw and treated data to GCS and BigQuery

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        filepath (str): Path to the saved treated .csv file
        raw_filepath (str, optional): Path to raw .json file. Defaults to None.
        partitions (str, optional): Partitioned directory structure, ie "ano=2022/mes=03/data=01".
        Defaults to None.

    Returns:
        None
    """
    log(
        f"""
    Received inputs:
    raw_filepath = {raw_filepath}, type = {type(raw_filepath)}
    treated_filepath = {filepath}, type = {type(filepath)}
    dataset_id = {dataset_id}, type = {type(dataset_id)}
    table_id = {table_id}, type = {type(table_id)}
    partitions = {partitions}, type = {type(partitions)}
    """
    )

    # Upload raw to staging
    if raw_filepath:
        st_obj = Storage(table_id=table_id, dataset_id=dataset_id)
        log(
            f"""Uploading raw file:
            {raw_filepath}
            to bucket {st_obj.bucket_name}
            at {st_obj.bucket_name}/{dataset_id}/{table_id}"""
        )
        st_obj.upload(
            path=raw_filepath, partitions=partitions, mode="raw", if_exists="replace"
        )

    # creates and publish table if it does not exist, append to it otherwise
    if partitions:
        # If table is partitioned, get parent directory wherein partitions are stored
        tb_dir = filepath.split(partitions)[0]
        create_or_append_table(dataset_id, table_id, tb_dir)
    else:
        create_or_append_table(dataset_id, table_id, filepath)

    # Delete local Files
    # log(f"Deleting local files: {raw_filepath}, {filepath}")
    # cleanup_local(filepath, raw_filepath)


@task
def bq_upload_from_dict(paths: dict, dataset_id: str, partition_levels: int = 1):
    """Upload multiple tables from a dict structured as {table_id: csv_path}.
        Present use case assumes table partitioned once. Adjust the parameter
        'partition_levels' to best suit new uses.
        i.e. if your csv is saved as:
            <table_id>/date=<run_date>/<filename>.csv
        it has 1 level of partition.
        if your csv file is saved as:
            <table_id>/date=<run_date>/hour=<run_hour>/<filename>.csv
        it has 2 levels of partition

    Args:
        paths (dict): _description_
        dataset_id (str): _description_

    Returns:
        _type_: _description_
    """
    for key in paths.keys():
        log("#" * 80)
        log(f"KEY = {key}")
        tb_dir = paths[key].parent
        # climb up the partition directories to reach the table dir
        for i in range(partition_levels):  # pylint: disable=unused-variable
            tb_dir = tb_dir.parent
        log(f"tb_dir = {tb_dir}")
        create_or_append_table(dataset_id=dataset_id, table_id=key, path=tb_dir)

    log(f"Returning -> {tb_dir.parent}")

    return tb_dir.parent


@task
def upload_logs_to_bq(dataset_id, parent_table_id, timestamp, error):
    """Upload execution status table to BigQuery.
    Table is uploaded to the same dataset, named {parent_table_id}_logs.


    Args:
        dataset_id (str): dataset_id on BigQuery
        parent_table_id (str): Parent table id related to the status table
        timestamp (str): ISO formatted timestamp string
        error (str): String associated with error caught during execution

    Returns:
        None
    """

    table_id = parent_table_id + "_logs"

    filepath = Path(
        f"{timestamp}/{table_id}/data={pendulum.parse(timestamp).date()}/{table_id}_{timestamp}.csv"
    )
    # create partition directory
    filepath.parent.mkdir(exist_ok=True, parents=True)
    # create dataframe to be uploaded
    dataframe = pd.DataFrame(
        {
            "timestamp_captura": [pd.to_datetime(timestamp)],
            "sucesso": [error is None],
            "erro": [error],
        }
    )
    # save local
    dataframe.to_csv(filepath, index=False)
    # BD Table object
    create_or_append_table(dataset_id=dataset_id, table_id=table_id, path=filepath)
    # delete local file
    # shutil.rmtree(f"{timestamp}")
