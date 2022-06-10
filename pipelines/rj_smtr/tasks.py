# -*- coding: utf-8 -*-
"""
Tasks for rj_smtr
"""
# pylint: disable=W0703

from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from typing import Union, List, Dict

from pytz import timezone

from basedosdados import Storage, Table
from dbt_client import DbtClient
import pandas as pd
import pendulum
from prefect import task
import requests

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import (
    create_or_append_table,
    bq_project,
    get_table_min_max_value,
    get_last_run_timestamp,
    parse_dbt_logs,
)
from pipelines.utils.execute_dbt_model.utils import get_dbt_client
from pipelines.utils.utils import log, get_vault_secret, get_redis_client

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
def run_dbt_model(  # pylint: disable=too-many-arguments
    dbt_client: DbtClient,
    model: str = None,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: Union[dict, List[Dict]] = None,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Runs a dbt command. If passing a dataset_id on model, will run the entire dataset.
    Otheerwise, if pasing a table_id, will run only the specified table.

    Args:
        dbt_client (DbtClient): Dbt interface of interaction
        model (str, optional): dataset_id or table_id on BigQuery. Defaults to None.
        table_id (str, optional): table_id on BigQuery, also .sql file name on your
        models folder. Defaults to None.
        command (str, optional): dbt command to run. Defaults to "run".
        flags (str, optional): flags allowed to the specific command.
        Should be preceeded by "--" Defaults to None.
        sync (bool, optional): _description_. Defaults to True.
    """
    run_command = "dbt run"

    # Set models and upstream/downstream for dbt
    if model:
        run_command += " --select "
        if upstream:
            run_command += "+"
        run_command += f"{model}"
        if downstream:
            run_command += "+"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        log(f"Received vars:\n {_vars}\n type: {type(_vars)}")
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                log(f"Received variable {elem}. Adding to vars")
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"
    if flags:
        run_command += f" {flags}"

    log(f"Will run the following command:\n{run_command}")
    logs_dict = dbt_client.cli(
        run_command,
        sync=True,
        logs=True,
    )
    parse_dbt_logs(logs_dict, log_queries=True)
    return log("Finished running dbt model")


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
    last_mat_date = get_table_min_max_value(
        query_project_id, dataset_id, mat_table_id, field_name, "max"
    )
    last_base_date = get_table_min_max_value(
        query_project_id, dataset_id, base_table_id, field_name, "max"
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
        last_mat_date = get_table_min_max_value(
            query_project_id, dataset_id, mat_table_id, field_name, "max"
        )

    if last_base_date > last_mat_date:
        log("Running interval step materialization")
        log(f"DBT will run the following command:\n{run_command}")
        while last_base_date > last_mat_date:
            running = dbt_client.cli(run_command, sync=True)
            last_mat_date = get_table_min_max_value(
                query_project_id,
                dataset_id,
                mat_table_id,
                field_name,
                "max",
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
    tz = constants.TIMEZONE.value  # pylint: disable=C0103

    capture_time = pendulum.now(tz)
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
def get_raw(url, headers=None, source: str = None):
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
    if source == "stpl_api":
        headers = get_vault_secret(source)["data"]
    if source == "sppo_api":
        access = get_vault_secret(source)["data"]
        key = list(access)[0]
        url = f"{url}{key}={access[key]}"
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
    create_or_append_table(
        dataset_id=dataset_id, table_id=table_id, path=filepath.parent.parent
    )


@task(
    checkpoint=False,
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def get_materialization_date_range(
    dataset_id: str,
    table_id: str,
    raw_dataset_id: str,
    raw_table_id: str,
    table_date_column_name: str = None,
):
    """
    Task for generating dict with variables to be passed to the
    --vars argument on DBT.

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): model filename on the queries repo.
        eg: if you have a model defined in the file <filename>.sql,
        the table_id should be <filename>
        table_date_column_name (Optional, str): if it's the first time this
        is ran, will query the table for the maximum value on this field.
        If rebuild is true, will query the table for the minimum value
        on this field.
        rebuild (Optional, bool): if true, queries the minimum date value on the
        table and return a date range from that value to the datetime.now() time

    Returns:
        dict: containing date_range_start and date_range_end
    """

    start_ts = get_last_run_timestamp(dataset_id=dataset_id, table_id=table_id)

    if start_ts is None:
        if Table(dataset_id=dataset_id, table_id=table_id).table_exists("prod"):
            start_ts = get_table_min_max_value(
                query_project_id=bq_project(),
                dataset_id=dataset_id,
                table_id=table_id,
                field_name=table_date_column_name,
                kind="max",
            ).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            start_ts = get_table_min_max_value(
                query_project_id=bq_project(),
                dataset_id=raw_dataset_id,
                table_id=raw_table_id,
                field_name=table_date_column_name,
                kind="max",
            ).strftime("%Y-%m-%dT%H:%M:%S")
    end_ts = datetime.now(timezone(constants.TIMEZONE.value)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    date_range = {"date_range_start": start_ts, "date_range_end": end_ts}
    return date_range


@task
def set_last_run_timestamp(
    dataset_id: str, table_id: str, wait=None
):  # pylint: disable=unused-argument
    """
    Set the `last_run_timestamp` key for the dataset_id/table_id pair
    to datetime.now() time. Used after running a materialization to set the
    stage for the next to come

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): model filename on the queries repo.
        wait (Any, optional): Used for defining dependencies inside the flow,
        in general, pass the output of the task which should be run imediately
        before this. Defaults to None.

    Returns:
        _type_: _description_
    """
    redis_client = get_redis_client()
    key = dataset_id + "." + table_id
    value = {
        "last_run_timestamp": datetime.now(timezone(constants.TIMEZONE.value)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
    }
    redis_client.set(key, value)
    return True


@task
def fetch_dataset_sha(dataset_id: str):
    """Fetches the SHA of a branch from Github"""
    url = "https://api.github.com/repos/prefeitura-rio/queries-rj-smtr"
    url += f"/commits?queries-rj-smtr/rj_smtr/{dataset_id}"
    response = requests.get(url)

    if response.status_code != 200:
        return None

    dataset_version = response.json()[0]["sha"]
    return {"version": dataset_version}
