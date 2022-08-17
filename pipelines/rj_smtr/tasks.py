# -*- coding: utf-8 -*-
"""
Tasks for rj_smtr
"""
# pylint: disable=W0703

from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import traceback
from typing import Union, List, Dict

from basedosdados import Storage, Table
import basedosdados as bd
from dbt_client import DbtClient
import pandas as pd
import pendulum
from prefect import task
from pytz import timezone
import requests

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import (
    create_or_append_table,
    bq_project,
    get_table_min_max_value,
    get_last_run_timestamp,
    # log_critical,
    parse_dbt_logs,
)
from pipelines.utils.execute_dbt_model.utils import get_dbt_client
from pipelines.utils.utils import log, get_redis_client

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
def get_current_timestamp(
    timestamp: datetime = None, truncate_minute: bool = True
) -> str:
    """
    Get current timestamp for flow run.
    """
    if not timestamp:
        timestamp = datetime.now(tz=timezone(constants.TIMEZONE.value))
    if truncate_minute:
        return timestamp.replace(second=0, microsecond=0).isoformat()
    return timestamp.isoformat()


@task
def create_date_hour_partition(timestamp_str: str) -> str:
    """
    Get date hour Hive partition structure from timestamp.
    """
    timestamp = datetime.fromisoformat(timestamp_str)
    return f"data={timestamp.strftime('%Y-%m-%d')}/hora={timestamp.strftime('%H')}"


@task
def parse_timestamp_to_string(timestamp: datetime, pattern="%Y-%m-%d-%H-%M-%S") -> str:
    """
    Parse timestamp to string pattern.
    """
    return timestamp.strftime(pattern)


@task
def create_current_date_hour_partition(capture_time=None):
    """Create partitioned directory structure to save data locally based
    on capture time.

    Args:
        capture_time(pendulum.datetime.DateTime, optional):
            if recapturing data, will create partitions based
            on the failed timestamps being recaptured

    Returns:
        dict: "filename" contains the name which to upload the csv, "partitions" contains
        the partitioned directory path
    """
    if capture_time is None:
        capture_time = datetime.now(tz=constants.TIMEZONE.value).replace(
            minute=0, second=0, microsecond=0
        )
    date = capture_time.strftime("%Y-%m-%d")
    hour = capture_time.strftime("%H")

    return {
        "filename": capture_time.strftime("%Y-%m-%d-%H-%M-%S"),
        "partitions": f"data={date}/hora={hour}",
        "timestamp": capture_time,
    }


@task
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
def save_raw_local(file_path: str, status: dict, mode: str = "raw") -> str:
    """
    Saves json response from API to .json file.

    Args:
        file_path (str): Path which to save raw file
        status (dict): Must contain keys
          * `data`: json returned from API
          * `error`: error catched from API request
        mode (str, optional): Folder to save locally, later folder which to upload to GCS.

    Returns:
        str: Path to the saved file
    """
    _file_path = file_path.format(mode=mode, filetype="json")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    if status["error"] is None:
        json.dump(status["data"], Path(_file_path).open("w", encoding="utf-8"))
        log(f"Raw data saved to: {_file_path}")
    return _file_path


@task
def save_treated_local(file_path: str, status: dict, mode: str = "staging") -> str:
    """
    Save treated file to CSV.

    Args:
        file_path (str): Path which to save treated file
        status (dict): Must contain keys
          * `data`: dataframe returned from treatement
          * `error`: error catched from data treatement
        mode (str, optional): Folder to save locally, later folder which to upload to GCS.

    Returns:
        str: Path to the saved file
    """
    _file_path = file_path.format(mode=mode, filetype="csv")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    if status["error"] is None:
        status["data"].to_csv(_file_path, index=False)
        log(f"Treated data saved to: {_file_path}")
    return _file_path


###############
#
# Extract data
#
###############
@task(nout=2)
def query_logs(
    dataset_id: str,
    table_id: str,
    datetime_filter: str = None,
) -> dict:
    """
    Queries capture logs to check for errors

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        datetime_filter (str, optional):
        ISO formatted datetime string filter passed to query.
        This task will query the logs table for the last 1 day before datetime_filter

    Returns:
        dict: containing the recapture parameters, 'timestamp', 'error' and
        'recapture'
    """

    if not datetime_filter:
        datetime_filter = pendulum.now(constants.TIMEZONE.value).replace(
            second=0, microsecond=0
        )
    else:
        datetime_filter = datetime.fromisoformat(datetime_filter)

    query = f"""
    with t as (
    select
        datetime(timestamp_array) as timestamp_array
    from
        unnest(GENERATE_TIMESTAMP_ARRAY(
            timestamp_sub('{datetime_filter.strftime('%Y-%m-%d %H:%M:%S')}', interval 1 day),
            timestamp('{datetime_filter.strftime('%Y-%m-%d %H:%M:%S')}'),
            interval 1 minute)
        ) as timestamp_array
    where timestamp_array < '{datetime_filter.strftime('%Y-%m-%d %H:%M:%S')}'
    ),
    logs as (
        select
            *,
            timestamp_trunc(timestamp_captura, minute) as timestamp_array
        from
            rj-smtr.{dataset_id}.{table_id}_logs
        where
            data between
                date(datetime_sub('{datetime_filter.strftime('%Y-%m-%d %H:%M:%S')}',
                interval 1 day))
                and date('{datetime_filter.strftime('%Y-%m-%d %H:%M:%S')}')
        and
            timestamp_captura between
                datetime_sub('{datetime_filter.strftime('%Y-%m-%d %H:%M:%S')}', interval 1 day)
                and '{datetime_filter.strftime('%Y-%m-%d %H:%M:%S')}'
        order by timestamp_captura
    )
    select
        case
            when logs.timestamp_captura is not null then logs.timestamp_captura
            else t.timestamp_array
        end as timestamp_captura,
        logs.erro
    from
        t
    left join
        logs
    on
        logs.timestamp_array = t.timestamp_array
    where
        logs.sucesso is not True
    order by
        timestamp_captura
    """
    log(f"Run query to check logs:\n{query}")
    results: pd.DataFrame = bd.read_sql(query=query, billing_project_id=bq_project())
    log(f"{results}")
    if len(results) > 0:
        results["timestamp_captura"] = (
            pd.to_datetime(results["timestamp_captura"])
            .dt.tz_localize(constants.TIMEZONE.value)
            .astype(str)
        )
        message = f"""
        [SPPO - Recaptures]
        Encontradas {len(results)} timestamps para serem recapturadas.
        Esta run processará as seguintes:
        {results.to_string()}
        """
        log(message)
        # log_critical(message=message)
        if len(results) > 40:
            message = f"""
            @here
            [SPPO - Recaptures]
            Encontradas {len(results)} timestamps para serem recapturadas.
            Esta run processará as seguintes:
            #####
            {results[:40].to_string()}
            #####
            Sobraram as seguintes para serem recapturadas na próxima run:
            #####
            {results[40:].to_string()}
            #####
            """
            # log_critical(message)
            results = results[:40]
        results.rename(
            columns={"timestamp_captura": "timestamp", "erro": "error"}, inplace=True
        )
        results["recapture"] = True
        return True, results.to_dict(orient="records")
    return False, {}


@task
def get_raw(url: str, headers: dict = None) -> Dict:
    """
    Request data from a url API.

    Args:
        url (str): URL to send request to
        headers (dict, optional): Aditional fields to send along the request.
    Returns:
        dict: Conatining keys
          * `data`: json returned from API
          * `error`: error catched from API request
    """
    data = None

    # Get data from API
    try:
        response = requests.get(
            url, headers=headers, timeout=constants.MAX_TIMEOUT_SECONDS.value
        )
        error = None
    except Exception as exp:
        error = exp
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")
        return {"data": None, "error": error}

    # Check data results
    if response.ok:  # status code is less than 400
        data = response.json()
        if isinstance(data, dict) and "DescricaoErro" in data.keys():
            error = data["DescricaoErro"]
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": data, "error": error}


###############
#
# Load data
#
###############


@task
def bq_upload(
    dataset_id: str,
    table_id: str,
    filepath: str,
    raw_filepath: str = None,
    partitions: str = None,
    status: dict = None,
):  # pylint: disable=R0913
    """
    Upload raw and treated data to GCS and BigQuery.

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        filepath (str): Path to the saved treated .csv file
        raw_filepath (str, optional): Path to raw .json file. Defaults to None.
        partitions (str, optional): Partitioned directory structure, ie "ano=2022/mes=03/data=01".
        Defaults to None.
        status (dict, optional): Dict containing `error` key from
        upstream tasks.

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
    if status["error"] is not None:
        return status["error"]

    error = None
    try:
        # Upload raw to staging
        if raw_filepath:
            st_obj = Storage(table_id=table_id, dataset_id=dataset_id)
            log(
                f"""Uploading raw file to bucket {st_obj.bucket_name} at
                {st_obj.bucket_name}/{dataset_id}/{table_id}"""
            )
            st_obj.upload(
                path=raw_filepath,
                partitions=partitions,
                mode="raw",
                if_exists="replace",
            )

        # Creates and publish table if it does not exist, append to it otherwise
        if partitions:
            # If table is partitioned, get parent directory wherein partitions are stored
            log(
                f"""Uploading treated partitions to bucket {st_obj.bucket_name}
            at {st_obj.bucket_name}/{dataset_id}/{table_id}"""
            )
            tb_dir = filepath.split(partitions)[0]
            create_or_append_table(dataset_id, table_id, tb_dir)
            # os.system(f'rm -rf {tb_dir}')
        else:
            create_or_append_table(dataset_id, table_id, filepath)
    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error


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
def upload_logs_to_bq(
    dataset_id: str,
    parent_table_id: str,
    timestamp: str,
    error: str = None,
    recapture: bool = False,
):
    """
    Upload execution status table to BigQuery.
    Table is uploaded to the same dataset, named {parent_table_id}_logs.
    If passing status_dict, should not pass timestamp and error.

    Args:
        dataset_id (str): dataset_id on BigQuery
        parent_table_id (str): Parent table id related to the status table
        timestamp (str): ISO formatted timestamp string
        error (str, optional): String associated with error caught during execution
    Returns:
        None
    """
    table_id = parent_table_id + "_logs"
    # Create partition directory
    filename = f"{table_id}_{timestamp.isoformat()}"
    filepath = Path(
        f"""data/staging/{dataset_id}/{table_id}/data={timestamp.date()}/{filename}.csv"""
    )
    filepath.parent.mkdir(exist_ok=True, parents=True)
    # Create dataframe to be uploaded
    if recapture is True:
        # if the recapture is succeeded, update the column erro
        if error is None:
            dataframe = pd.DataFrame(
                {
                    "timestamp_captura": [timestamp],
                    "sucesso": [True],
                    "erro": ["[recapturado]"],
                }
            )
        # if any iteration of the recapture fails, upload logs with error
        else:
            dataframe = pd.DataFrame(
                {
                    "timestamp_captura": [timestamp],
                    "sucesso": [error is None],
                    "erro": [f"[recapturado] {error}"],
                }
            )
    else:
        dataframe = pd.DataFrame(
            {
                "timestamp_captura": [timestamp],
                "sucesso": [error is None],
                "erro": [error],
            }
        )
    # Save data local
    dataframe.to_csv(filepath, index=False)
    # Upload to Storage
    create_or_append_table(
        dataset_id=dataset_id, table_id=table_id, path=filepath.parent.parent
    )
    if error is not None:
        raise Exception(f"Pipeline failed with error: {error}")


@task(
    checkpoint=False,
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def get_materialization_date_range(  # pylint: disable=R0913
    dataset_id: str,
    table_id: str,
    raw_dataset_id: str,
    raw_table_id: str,
    table_date_column_name: str = None,
    mode: str = "prod",
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
    timestr = "%Y-%m-%dT%H:%M:%S"
    # get start from redis
    last_run = get_last_run_timestamp(
        dataset_id=dataset_id, table_id=table_id, mode=mode
    )
    # if there's no timestamp set on redis, get max timestamp on source table
    if last_run is None:
        if Table(dataset_id=dataset_id, table_id=table_id).table_exists("prod"):
            last_run = get_table_min_max_value(
                query_project_id=bq_project(),
                dataset_id=dataset_id,
                table_id=table_id,
                field_name=table_date_column_name,
                kind="max",
            ).strftime(timestr)
        else:
            last_run = get_table_min_max_value(
                query_project_id=bq_project(),
                dataset_id=raw_dataset_id,
                table_id=raw_table_id,
                field_name=table_date_column_name,
                kind="max",
            ).strftime(timestr)
    # set start to last run hour (H)
    start_ts = (
        (datetime.strptime(last_run, timestr))
        .replace(minute=0, second=0, microsecond=0)
        .strftime(timestr)
    )
    # set end to H+60
    end_ts = (
        (datetime.strptime(last_run, timestr) + timedelta(minutes=60))
        .replace(minute=0, second=0, microsecond=0)
        .strftime(timestr)
    )

    date_range = {"date_range_start": start_ts, "date_range_end": end_ts}
    return date_range


@task
def set_last_run_timestamp(
    dataset_id: str, table_id: str, timestamp: str, mode: str = "prod", wait=None
):  # pylint: disable=unused-argument
    """
    Set the `last_run_timestamp` key for the dataset_id/table_id pair
    to datetime.now() time. Used after running a materialization to set the
    stage for the next to come

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): model filename on the queries repo.
        timestamp: Last run timestamp end.
        wait (Any, optional): Used for defining dependencies inside the flow,
        in general, pass the output of the task which should be run imediately
        before this. Defaults to None.

    Returns:
        _type_: _description_
    """
    redis_client = get_redis_client()
    key = dataset_id + "." + table_id
    if mode == "dev":
        key = f"{mode}.{key}"
    value = {
        "last_run_timestamp": timestamp,
    }
    redis_client.set(key, value)
    return True


@task
def delay_now_time(timestamp: str, delay_minutes=6):
    """Return timestamp string delayed by <delay_minutes>

    Args:
        timestamp (str): Isoformat timestamp string
        delay_minutes (int, optional): Minutes to delay timestamp by Defaults to 6.

    Returns:
        str : timestamp string formatted as "%Y-%m-%dT%H-%M-%S"
    """
    ts_obj = datetime.fromisoformat(timestamp)
    ts_obj = ts_obj - timedelta(minutes=delay_minutes)
    return ts_obj.strftime("%Y-%m-%dT%H-%M-%S")


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
