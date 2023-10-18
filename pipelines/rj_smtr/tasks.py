# -*- coding: utf-8 -*-
# pylint: disable=W0703, W0511
"""
Tasks for rj_smtr
"""
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import traceback
from typing import Dict, List, Union, Iterable, Any
import io

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
    data_info_str,
    dict_contains_keys,
    get_raw_data_api,
    get_raw_data_gcs,
    get_raw_data_db,
    upload_run_logs_to_bq,
    get_datetime_range,
    read_raw_data,
    save_treated_local_func,
    save_raw_local_func,
    query_logs_func,
)
from pipelines.utils.execute_dbt_model.utils import get_dbt_client
from pipelines.utils.utils import log, get_redis_client, get_vault_secret

from pipelines.utils.tasks import get_now_date

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


@task(checkpoint=False, nout=3)
def create_dbt_run_vars(
    dataset_id: str,
    dbt_vars: dict,
    table_id: str,
    raw_dataset_id: str,
    raw_table_id: str,
    mode: str,
) -> tuple[list[dict], Union[list[dict], dict, None], bool]:
    """
    Create the variables to be used in dbt materialization based on a dict

    Args:
        dataset_id (str): the dataset_id to get the variables
        dbt_vars (dict): dict containing the parameters
        table_id (str): the table_id get the date_range variable
        raw_dataset_id (str): the raw_dataset_id get the date_range variable
        raw_table_id (str): the raw_table_id get the date_range variable
        mode (str): the mode to get the date_range variable

    Returns:
        tuple[list[dict]: the variables to be used in DBT
        Union[list[dict], dict, None]: the date variable (date_range or run_date)
        bool: a flag that indicates if the date_range variable came from Redis
    """

    log(f"Creating DBT variables. Parameter received: {dbt_vars}")

    if (not dbt_vars) or (not table_id):
        log("dbt_vars or table_id are blank. Skiping task")
        return [None], None, False

    final_vars = []
    date_var = None
    flag_date_range = False

    if "date_range" in dbt_vars.keys():
        log("Creating date_range variable")

        # Set date_range variable manually
        if dict_contains_keys(
            dbt_vars["date_range"], ["date_range_start", "date_range_end"]
        ):
            date_var = {
                "date_range_start": dbt_vars["date_range"]["date_range_start"],
                "date_range_end": dbt_vars["date_range"]["date_range_end"],
            }
        # Create date_range using Redis
        else:
            raw_table_id = raw_table_id or table_id

            date_var = get_materialization_date_range.run(
                dataset_id=dataset_id,
                table_id=table_id,
                raw_dataset_id=raw_dataset_id,
                raw_table_id=raw_table_id,
                table_run_datetime_column_name=dbt_vars["date_range"].get(
                    "table_run_datetime_column_name"
                ),
                mode=mode,
                delay_hours=dbt_vars["date_range"].get("delay_hours", 0),
            )

            flag_date_range = True

        final_vars.append(date_var.copy())

        log(f"date_range created: {date_var}")

    elif "run_date" in dbt_vars.keys():
        log("Creating run_date variable")

        date_var = get_run_dates.run(
            dbt_vars["run_date"].get("date_range_start"),
            dbt_vars["run_date"].get("date_range_end"),
        )
        final_vars.append([d.copy() for d in date_var])

        log(f"run_date created: {date_var}")

    if "version" in dbt_vars.keys():
        log("Creating version variable")
        dataset_sha = fetch_dataset_sha.run(dataset_id=dataset_id)

        # if there are other variables inside the list, update each item adding the version variable
        if final_vars:
            final_vars = get_join_dict.run(dict_list=final_vars, new_dict=dataset_sha)
        else:
            final_vars.append(dataset_sha)

        log(f"version created: {dataset_sha}")

    log(f"All variables was created, final value is: {final_vars}")

    return final_vars, date_var, flag_date_range


###############
#
# Local file managment
#
###############


@task
def get_current_timestamp(timestamp=None, truncate_minute: bool = True) -> datetime:
    """
    Get current timestamp for flow run.

    Args:
        timestamp: timestamp to be used as reference (optionally, it can be a string)
        truncate_minute: whether to truncate the timestamp to the minute or not

    Returns:
        datetime: timestamp for flow run
    """
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)
    if not timestamp:
        timestamp = datetime.now(tz=timezone(constants.TIMEZONE.value))
    if truncate_minute:
        return timestamp.replace(second=0, microsecond=0)
    return timestamp


@task
def create_date_hour_partition(
    timestamp: datetime, partition_date_only: bool = False
) -> str:
    """
    Create a date (and hour) Hive partition structure from timestamp.

    Args:
        timestamp (datetime): timestamp to be used as reference
        partition_date_only (bool, optional): whether to add hour partition or not

    Returns:
        str: partition string
    """
    partition = f"data={timestamp.strftime('%Y-%m-%d')}"
    if not partition_date_only:
        partition += f"/hora={timestamp.strftime('%H')}"
    return partition


@task
def parse_timestamp_to_string(timestamp: datetime, pattern="%Y-%m-%d-%H-%M-%S") -> str:
    """
    Parse timestamp to string pattern.
    """
    return timestamp.strftime(pattern)


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
    file_path = f"{os.getcwd()}/{data_folder}/{{mode}}/{dataset_id}_dev/{table_id}"
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
          * data: json returned from API
          * error: error catched from API request
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
@task(nout=3)
def query_logs(
    dataset_id: str,
    table_id: str,
    datetime_filter=None,
    max_recaptures: int = 60,
    interval_minutes: int = 1,
    recapture_window_days: int = 1,
):
    """
    Queries capture logs to check for errors

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        datetime_filter (pendulum.datetime.DateTime, optional):
        filter passed to query. This task will query the logs table
        for the last n (n = recapture_window_days) days before datetime_filter
        max_recaptures (int, optional): maximum number of recaptures to be done
        interval_minutes (int, optional): interval in minutes between each recapture
        recapture_window_days (int, optional): Number of days to query for erros

    Returns:
        lists: errors (bool),
        timestamps (list of pendulum.datetime.DateTime),
        previous_errors (list of previous errors)
    """

    return query_logs_func(
        dataset_id=dataset_id,
        table_id=table_id,
        datetime_filter=datetime_filter,
        max_recaptures=max_recaptures,
        interval_minutes=interval_minutes,
        recapture_window_days=recapture_window_days,
    )


@task
def get_raw(  # pylint: disable=R0912
    url: str,
    headers: str = None,
    filetype: str = "json",
    csv_args: dict = None,
    params: dict = None,
) -> Dict:
    """
    Request data from URL API

    Args:
        url (str): URL to send request
        headers (str, optional): Path to headers guardeded on Vault, if needed.
        filetype (str, optional): Filetype to be formatted (supported only: json, csv and txt)
        csv_args (dict, optional): Arguments for read_csv, if needed
        params (dict, optional): Params to be sent on request

    Returns:
        dict: Conatining keys
          * `data` (json): data result
          * `error` (str): catched error, if any. Otherwise, returns None
    """
    data = None
    error = None

    try:
        if headers is not None:
            headers = get_vault_secret(headers)["data"]

            # remove from headers, if present
            remove_headers = ["host", "databases"]
            for remove_header in remove_headers:
                if remove_header in list(headers.keys()):
                    del headers[remove_header]

        response = requests.get(
            url,
            headers=headers,
            timeout=constants.MAX_TIMEOUT_SECONDS.value,
            params=params,
        )

        if response.ok:  # status code is less than 400
            if filetype == "json":
                data = response.json()

                # todo: move to data check on specfic API # pylint: disable=W0102
                if isinstance(data, dict) and "DescricaoErro" in data.keys():
                    error = data["DescricaoErro"]

            elif filetype in ("txt", "csv"):
                if csv_args is None:
                    csv_args = {}
                data = pd.read_csv(io.StringIO(response.text), **csv_args).to_dict(
                    orient="records"
                )
            else:
                error = (
                    "Unsupported raw file extension. Supported only: json, csv and txt"
                )

    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": data, "error": error}


@task(checkpoint=False, nout=2)
def create_request_params(
    extract_params: dict,
    table_id: str,
    dataset_id: str,
    timestamp: datetime,
    interval_minutes: int,
) -> tuple[str, str]:
    """
    Task to create request params

    Args:
        extract_params (dict): extract parameters
        table_id (str): table_id on BigQuery
        dataset_id (str): dataset_id on BigQuery
        timestamp (datetime): timestamp for flow run
        interval_minutes (int): interval in minutes between each capture

    Returns:
        request_params: host, database and query to request data
        request_url: url to request data
    """
    request_params = None
    request_url = None

    if dataset_id == constants.BILHETAGEM_DATASET_ID.value:
        database = constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value["databases"][
            extract_params["database"]
        ]
        request_url = database["host"]

        datetime_range = get_datetime_range(
            timestamp=timestamp, interval=timedelta(minutes=interval_minutes)
        )

        request_params = {
            "database": extract_params["database"],
            "engine": database["engine"],
            "query": extract_params["query"].format(**datetime_range),
        }

    return request_params, request_url


@task(checkpoint=False, nout=2)
def get_raw_from_sources(
    source_type: str,
    local_filepath: str,
    source_path: str = None,
    dataset_id: str = None,
    table_id: str = None,
    secret_path: str = None,
    request_params: dict = None,
) -> tuple[str, str]:
    """
    Task to get raw data from sources

    Args:
        source_type (str): source type
        local_filepath (str): local filepath
        source_path (str, optional): source path. Defaults to None.
        dataset_id (str, optional): dataset_id on BigQuery. Defaults to None.
        table_id (str, optional): table_id on BigQuery. Defaults to None.
        secret_path (str, optional): secret path. Defaults to None.
        request_params (dict, optional): request parameters. Defaults to None.

    Returns:
        error: error catched from upstream tasks
        filepath: filepath to raw data
    """
    error = None
    filepath = None
    data = None

    source_values = source_type.split("-", 1)

    source_type, filetype = (
        source_values if len(source_values) == 2 else (source_values[0], None)
    )

    log(f"Getting raw data from source type: {source_type}")

    try:
        if source_type == "api":
            error, data, filetype = get_raw_data_api(
                url=source_path,
                secret_path=secret_path,
                api_params=request_params,
                filetype=filetype,
            )
        elif source_type == "gcs":
            error, data, filetype = get_raw_data_gcs(
                dataset_id=dataset_id, table_id=table_id, zip_filename=request_params
            )
        elif source_type == "db":
            error, data, filetype = get_raw_data_db(
                host=source_path, secret_path=secret_path, **request_params
            )
        else:
            raise NotImplementedError(f"{source_type} not supported")

        filepath = save_raw_local_func(
            data=data, filepath=local_filepath, filetype=filetype
        )

    except NotImplementedError:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    log(f"Raw extraction ended returned values: {error}, {filepath}")
    return error, filepath


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
        create_or_append_table(
            dataset_id=dataset_id,
            table_id=table_id,
            path=filepath,
            partitions=partitions,
        )
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
def upload_logs_to_bq(  # pylint: disable=R0913
    dataset_id: str,
    parent_table_id: str,
    timestamp: str,
    error: str = None,
    previous_error: str = None,
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
    partition = f"data={timestamp.date()}"
    filepath = Path(
        f"""data/staging/{dataset_id}/{table_id}/{partition}/{filename}.csv"""
    )
    filepath.parent.mkdir(exist_ok=True, parents=True)
    # Create dataframe to be uploaded
    if not error and recapture is True:
        # if the recapture is succeeded, update the column erro
        dataframe = pd.DataFrame(
            {
                "timestamp_captura": [timestamp],
                "sucesso": [True],
                "erro": [f"[recapturado]{previous_error}"],
            }
        )
        log(f"Recapturing {timestamp} with previous error:\n{error}")
    else:
        # not recapturing or error during flow execution
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
        dataset_id=dataset_id,
        table_id=table_id,
        path=filepath.as_posix(),
        partitions=partition,
    )
    if error is not None:
        raise Exception(f"Pipeline failed with error: {error}")


@task
def upload_raw_data_to_gcs(
    error: str,
    raw_filepath: str,
    table_id: str,
    dataset_id: str,
    partitions: list,
) -> Union[str, None]:
    """
    Upload raw data to GCS.

    Args:
        error (str): Error catched from upstream tasks.
        raw_filepath (str): Path to the saved raw .json file
        table_id (str): table_id on BigQuery
        dataset_id (str): dataset_id on BigQuery
        partitions (list): list of partition strings

    Returns:
        Union[str, None]: if there is an error returns it traceback, otherwise returns None
    """
    if error is None:
        try:
            st_obj = Storage(table_id=table_id, dataset_id=dataset_id)
            log(
                f"""Uploading raw file to bucket {st_obj.bucket_name} at
                {st_obj.bucket_name}/{dataset_id}_dev/{table_id}"""
            )
            st_obj.upload(
                path=raw_filepath,
                partitions=partitions,
                mode="raw",
                if_exists="replace",
            )
        except Exception:
            error = traceback.format_exc()
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error


@task
def upload_staging_data_to_gcs(
    error: str,
    staging_filepath: str,
    timestamp: datetime,
    table_id: str,
    dataset_id: str,
    partitions: list,
    previous_error: str = None,
    recapture: bool = False,
) -> Union[str, None]:
    """
    Upload staging data to GCS.

    Args:
        error (str): Error catched from upstream tasks.
        staging_filepath (str): Path to the saved treated .csv file.
        timestamp (datetime): timestamp for flow run.
        table_id (str): table_id on BigQuery.
        dataset_id (str): dataset_id on BigQuery.
        partitions (list): list of partition strings.

    Returns:
        Union[str, None]: if there is an error returns it traceback, otherwise returns None
    """
    if error is None:
        try:
            # Creates and publish table if it does not exist, append to it otherwise
            create_or_append_table(
                dataset_id=f"{dataset_id}_dev",
                table_id=table_id,
                path=staging_filepath,
                partitions=partitions,
            )
        except Exception:
            error = traceback.format_exc()
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    log(f"previous_error = {previous_error}")

    upload_run_logs_to_bq(
        dataset_id=f"{dataset_id}_dev",
        parent_table_id=table_id,
        error=error,
        timestamp=timestamp,
        mode="staging",
        previous_error=previous_error,
        recapture=recapture,
    )

    return error


###############
#
# Daterange tasks
#
###############


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
    table_run_datetime_column_name: str = None,
    mode: str = "prod",
    delay_hours: int = 0,
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
        delay(Optional, int): hours delayed from now time for materialization range
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
        log("Failed to fetch key from Redis...\n Querying tables for last suceeded run")
        if Table(dataset_id=dataset_id, table_id=table_id).table_exists("prod"):
            last_run = get_table_min_max_value(
                query_project_id=bq_project(),
                dataset_id=dataset_id,
                table_id=table_id,
                field_name=table_run_datetime_column_name,
                kind="max",
            )
            log(
                f"""
            Queried last run from {dataset_id}.{table_id}
            Got:
            {last_run} as type {type(last_run)}
            """
            )
        else:
            last_run = get_table_min_max_value(
                query_project_id=bq_project(),
                dataset_id=raw_dataset_id,
                table_id=raw_table_id,
                field_name=table_run_datetime_column_name,
                kind="max",
            )
        log(
            f"""
            Queried last run from {raw_dataset_id}.{raw_table_id}
            Got:
            {last_run} as type {type(last_run)}
            """
        )
    else:
        last_run = datetime.strptime(last_run, timestr)

    # set start to last run hour (H)
    start_ts = last_run.replace(minute=0, second=0, microsecond=0).strftime(timestr)

    # set end to now - delay
    now_ts = pendulum.now(constants.TIMEZONE.value).replace(
        tzinfo=None, minute=0, second=0, microsecond=0
    )
    end_ts = (
        (now_ts - timedelta(hours=delay_hours))
        .replace(minute=0, second=0, microsecond=0)
        .strftime(timestr)
    )
    date_range = {"date_range_start": start_ts, "date_range_end": end_ts}
    log(f"Got date_range as: {date_range}")
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
    content = redis_client.get(key)
    if not content:
        content = {}
    content["last_run_timestamp"] = timestamp
    redis_client.set(key, content)
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


@task
def get_run_dates(date_range_start: str, date_range_end: str) -> List:
    """
    Generates a list of dates between date_range_start and date_range_end.
    """
    if (date_range_start is False) or (date_range_end is False):
        dates = [{"run_date": get_now_date.run()}]
    else:
        dates = [
            {"run_date": d.strftime("%Y-%m-%d")}
            for d in pd.date_range(start=date_range_start, end=date_range_end)
        ]
    log(f"Will run the following dates: {dates}")
    return dates


@task
def get_join_dict(dict_list: list, new_dict: dict) -> List:
    """
    Updates a list of dictionaries with a new dictionary.
    """
    for dict_temp in dict_list:
        dict_temp.update(new_dict)

    log(f"get_join_dict: {dict_list}")
    return dict_list


@task(checkpoint=False)
def get_previous_date(days):
    """
    Returns the date of {days} days ago in YYYY-MM-DD.
    """
    now = pendulum.now(pendulum.timezone("America/Sao_Paulo")).subtract(days=days)

    return now.to_date_string()


###############
#
# Pretreat data
#
###############


@task(nout=2)
def transform_raw_to_nested_structure(
    raw_filepath: str,
    filepath: str,
    error: str,
    timestamp: datetime,
    primary_key: list = None,
) -> tuple[str, str]:
    """
    Task to transform raw data to nested structure

    Args:
        raw_filepath (str): Path to the saved raw .json file
        filepath (str): Path to the saved treated .csv file
        error (str): Error catched from upstream tasks
        timestamp (datetime): timestamp for flow run
        primary_key (list, optional): Primary key to be used on nested structure

    Returns:
        str: Error traceback
        str: Path to the saved treated .csv file
    """
    if error is None:
        try:
            # leitura do dado raw
            error, data = read_raw_data(filepath=raw_filepath)

            if primary_key is None:
                primary_key = []

            log(
                f"""
                Received inputs:
                - timestamp:\n{timestamp}
                - data:\n{data.head()}"""
            )

            # Check empty dataframe
            if data.empty:
                log("Empty dataframe, skipping transformation...")
            else:
                log(f"Raw data:\n{data_info_str(data)}", level="info")

                log("Adding captured timestamp column...", level="info")
                data["timestamp_captura"] = timestamp

                log("Striping string columns...", level="info")
                for col in data.columns[data.dtypes == "object"].to_list():
                    data[col] = data[col].str.strip()

                log(f"Finished cleaning! Data:\n{data_info_str(data)}", level="info")

                log("Creating nested structure...", level="info")
                pk_cols = primary_key + ["timestamp_captura"]
                data = (
                    data.groupby(pk_cols)
                    .apply(
                        lambda x: x[data.columns.difference(pk_cols)].to_json(
                            orient="records"
                        )
                    )
                    .str.strip("[]")
                    .reset_index(name="content")[
                        primary_key + ["content", "timestamp_captura"]
                    ]
                )

                log(
                    f"Finished nested structure! Data:\n{data_info_str(data)}",
                    level="info",
                )

            # save treated local
            filepath = save_treated_local_func(
                data=data, error=error, filepath=filepath
            )

        except Exception:  # pylint: disable=W0703
            error = traceback.format_exc()
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error, filepath


###############
#
# Utilitary tasks
#
###############


@task(checkpoint=False)
def coalesce_task(value_list: Iterable):
    """
    Task to get the first non None value of a list

    Args:
        value_list (Iterable): a iterable object with the values
    Returns:
        any: value_list's first non None item
    """

    try:
        return next(value for value in value_list if value is not None)
    except StopIteration:
        return None


@task(checkpoint=False, nout=2)
def unpack_mapped_results_nout2(
    mapped_results: Iterable,
) -> tuple[list[Any], list[Any]]:
    """
    Task to unpack the results from an nout=2 tasks in 2 lists when it is mapped

    Args:
        mapped_results (Iterable): The mapped task return

    Returns:
        tuple[list[Any], list[Any]]: The task original return splited in 2 lists:
            - 1st list being all the first return
            - 2nd list being all the second return

    """
    return [r[0] for r in mapped_results], [r[1] for r in mapped_results]
