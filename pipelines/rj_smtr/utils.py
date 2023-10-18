# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
General purpose functions for rj_smtr
"""

from ftplib import FTP
from pathlib import Path

from datetime import timedelta, datetime, date
import pendulum
from typing import List, Union, Any
import traceback
import io
import json
import zipfile
import pytz
import requests
import basedosdados as bd
from basedosdados import Table
import pandas as pd
from google.cloud.storage.blob import Blob
import pymysql
import psycopg2
import psycopg2.extras


from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants as emd_constants

from pipelines.rj_smtr.implicit_ftp import ImplicitFtpTls
from pipelines.rj_smtr.constants import constants

from pipelines.utils.utils import (
    log,
    get_vault_secret,
    send_discord_message,
    get_redis_client,
)


# Set BD config to run on cloud #
bd.config.from_file = True


def log_critical(message: str, secret_path: str = constants.CRITICAL_SECRET_PATH.value):
    """Logs message to critical discord channel specified

    Args:
        message (str): Message to post on the channel
        secret_path (str, optional): Secret path storing the webhook to critical channel.
        Defaults to constants.CRITICAL_SECRETPATH.value.

    """
    url = get_vault_secret(secret_path=secret_path)["data"]["url"]
    return send_discord_message(message=message, webhook_url=url)


def create_or_append_table(
    dataset_id: str, table_id: str, path: str, partitions: str = None
):
    """Conditionally create table or append data to its relative GCS folder.

    Args:
        dataset_id (str): target dataset_id on BigQuery
        table_id (str): target table_id on BigQuery
        path (str): Path to .csv data file
    """
    tb_obj = Table(table_id=table_id, dataset_id=dataset_id)
    if not tb_obj.table_exists("staging"):
        log("Table does not exist in STAGING, creating table...")
        dirpath = path.split(partitions)[0]
        tb_obj.create(
            path=dirpath,
            if_table_exists="pass",
            if_storage_data_exists="replace",
        )
        log("Table created in STAGING")
    else:
        log("Table already exists in STAGING, appending to it...")
        tb_obj.append(
            filepath=path, if_exists="replace", timeout=600, partitions=partitions
        )
        log("Appended to table on STAGING successfully.")


def generate_df_and_save(data: dict, fname: Path):
    """Save DataFrame as csv

    Args:
        data (dict): dict with the data which to build the DataFrame
        fname (Path): _description_
    """
    # Generate dataframe
    dataframe = pd.DataFrame()
    dataframe[data["key_column"]] = [
        piece[data["key_column"]] for piece in data["data"]
    ]
    dataframe["content"] = list(data["data"])

    # Save dataframe to CSV
    dataframe.to_csv(fname, index=False)


def bq_project(kind: str = "bigquery_prod"):
    """Get the set BigQuery project_id

    Args:
        kind (str, optional): Which client to get the project name from.
        Options are 'bigquery_staging', 'bigquery_prod' and 'storage_staging'
        Defaults to 'bigquery_prod'.

    Returns:
        str: the requested project_id
    """
    return bd.upload.base.Base().client[kind].project


def get_table_min_max_value(  # pylint: disable=R0913
    query_project_id: str,
    dataset_id: str,
    table_id: str,
    field_name: str,
    kind: str,
    wait=None,  # pylint: disable=unused-argument
):
    """Query a table to get the maximum value for the chosen field.
    Useful to incrementally materialize tables via DBT

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        field_name (str): column name to query
        kind (str): which value to get. Accepts min and max
    """
    log(f"Getting {kind} value for {table_id}")
    query = f"""
        SELECT
            {kind}({field_name})
        FROM {query_project_id}.{dataset_id}.{table_id}
    """
    log(f"Will run query:\n{query}")
    result = bd.read_sql(query=query, billing_project_id=bq_project())

    return result.iloc[0][0]


def get_last_run_timestamp(dataset_id: str, table_id: str, mode: str = "prod") -> str:
    """
    Query redis to retrive the time for when the last materialization
    ran.

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): model filename on the queries repo.
        eg: if you have a model defined in the file <filename>.sql,
        the table_id should be <filename>
        mode (str):

    Returns:
        Union[str, None]: _description_
    """
    redis_client = get_redis_client()
    key = dataset_id + "." + table_id
    log(f"Fetching key {key} from redis, working on mode {mode}")
    if mode == "dev":
        key = f"{mode}.{key}"
    runs = redis_client.get(key)
    # if runs is None:
    #     redis_client.set(key, "")
    try:
        last_run_timestamp = runs["last_run_timestamp"]
    except KeyError:
        return None
    except TypeError:
        return None
    log(f"Got value {last_run_timestamp}")
    return last_run_timestamp


def map_dict_keys(data: dict, mapping: dict) -> None:
    """
    Map old keys to new keys in a dict.
    """
    for old_key, new_key in mapping.items():
        data[new_key] = data.pop(old_key)
    return data


def connect_ftp(secret_path: str = None, secure: bool = True):
    """Connect to FTP

    Returns:
        ImplicitFTP_TLS: ftp client
    """

    ftp_data = get_vault_secret(secret_path)["data"]
    if secure:
        ftp_client = ImplicitFtpTls()
    else:
        ftp_client = FTP()
    ftp_client.connect(host=ftp_data["host"], port=int(ftp_data["port"]))
    ftp_client.login(user=ftp_data["username"], passwd=ftp_data["pwd"])
    if secure:
        ftp_client.prot_p()
    return ftp_client


def safe_cast(val, to_type, default=None):
    """
    Safe cast value.
    """
    try:
        return to_type(val)
    except ValueError:
        return default


def set_redis_rdo_files(redis_client, dataset_id: str, table_id: str):
    """
    Register downloaded files to Redis

    Args:
        redis_client (_type_): _description_
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery

    Returns:
        bool: if the key was properly set
    """
    try:
        content = redis_client.get(f"{dataset_id}.{table_id}")["files"]
    except TypeError as e:
        log(f"Caught error {e}. Will set unexisting key")
        # set key to empty dict for filling later
        redis_client.set(f"{dataset_id}.{table_id}", {"files": []})
        content = redis_client.get(f"{dataset_id}.{table_id}")
    # update content
    st_client = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    blob_names = [
        blob.name
        for blob in st_client.client["storage_staging"].list_blobs(
            st_client.bucket, prefix=f"staging/{dataset_id}/{table_id}"
        )
    ]
    files = [blob_name.split("/")[-1].replace(".csv", "") for blob_name in blob_names]
    log(f"When setting key, found {len(files)} files. Will register on redis...")
    content["files"] = files
    # set key
    return redis_client.set(f"{dataset_id}.{table_id}", content)


# PRE TREAT #


def check_not_null(data: pd.DataFrame, columns: list, subset_query: str = None):
    """
    Check if there are null values in columns.

    Args:
        columns (list): list of columns to check
        subset_query (str): query to check if there are important data
        being removed

    Returns:
        None
    """

    for col in columns:
        remove = data.query(f"{col} != {col}")  # null values
        log(
            f"[data-check] There are {len(remove)} rows with null values in '{col}'",
            level="info",
        )

        if subset_query is not None:
            # Check if there are important data being removed
            remove = remove.query(subset_query)
            if len(remove) > 0:
                log(
                    f"""[data-check] There are {len(remove)} critical rows with
                    null values in '{col}' (query: {subset_query})""",
                    level="warning",
                )


def filter_null(data: pd.DataFrame, columns: list, subset_query: str = None):
    """
    Filter null values in columns.

    Args:
        columns (list): list of columns to check
        subset_query (str): query to check if there are important data
        being removed

    Returns:
        pandas.DataFrame: data without null values
    """

    for col in columns:
        remove = data.query(f"{col} != {col}")  # null values
        data = data.drop(remove.index)
        log(
            f"[data-filter] Removed {len(remove)} rows with null '{col}'",
            level="info",
        )

        if subset_query is not None:
            # Check if there are important data being removed
            remove = remove.query(subset_query)
            if len(remove) > 0:
                log(
                    f"[data-filter] Removed {len(remove)} critical rows with null '{col}'",
                    level="warning",
                )

    return data


def filter_data(data: pd.DataFrame, filters: list, subset_query: str = None):
    """
    Filter data from a dataframe

    Args:
        data (pd.DataFrame): data DataFrame
        filters (list): list of queries to filter data

    Returns:
        pandas.DataFrame: data without filter data
    """
    for item in filters:
        remove = data.query(item)
        data = data.drop(remove.index)
        log(
            f"[data-filter] Removed {len(remove)} rows from filter: {item}",
            level="info",
        )

        if subset_query is not None:
            # Check if there are important data being removed
            remove = remove.query(subset_query)
            if len(remove) > 0:
                log(
                    f"""[data-filter] Removed {len(remove)} critical rows
                    from filter: {item} (subquery: {subset_query})""",
                    level="warning",
                )

    return data


def check_relation(data: pd.DataFrame, columns: list):
    """
    Check relation between collumns.

    Args:
        data (pd.DataFrame): dataframe to be modified
        columns (list): list of lists of columns to be checked

    Returns:
        None
    """

    for cols in columns:
        df_dup = (
            data[~data.duplicated(subset=cols)]
            .groupby(cols)
            .count()
            .reset_index()
            .iloc[:, :1]
        )

        for col in cols:
            df_dup_col = (
                data[~data.duplicated(subset=col)]
                .groupby(col)
                .count()
                .reset_index()
                .iloc[:, :1]
            )

            if len(df_dup_col[~df_dup_col[col].duplicated()]) == len(df_dup):
                log(
                    f"[data-check] Comparing '{col}' in '{cols}', there are no duplicated values",
                    level="info",
                )
            else:
                log(
                    f"[data-check] Comparing '{col}' in '{cols}', there are duplicated values",
                    level="warning",
                )


def data_info_str(data: pd.DataFrame):
    """
    Return dataframe info as a str to log

    Args:
        data (pd.DataFrame): dataframe

    Returns:
        data.info() as a string
    """
    buffer = io.StringIO()
    data.info(buf=buffer)
    return buffer.getvalue()


def generate_execute_schedules(  # pylint: disable=too-many-arguments,too-many-locals
    clock_interval: timedelta,
    labels: List[str],
    table_parameters: Union[list[dict], dict],
    runs_interval_minutes: int = 15,
    start_date: datetime = datetime(
        2020, 1, 1, tzinfo=pytz.timezone(emd_constants.DEFAULT_TIMEZONE.value)
    ),
    **general_flow_params,
) -> List[IntervalClock]:
    """
    Generates multiple schedules

    Args:
        clock_interval (timedelta): The interval to run the schedule
        labels (List[str]): The labels to be added to the schedule
        table_parameters (list): The table parameters to iterate over
        runs_interval_minutes (int, optional): The interval between each schedule. Defaults to 15.
        start_date (datetime, optional): The start date of the schedule.
            Defaults to datetime(2020, 1, 1, tzinfo=pytz.timezone(emd_constants.DEFAULT_TIMEZONE.value)).
        general_flow_params: Any param that you want to pass to the flow
    Returns:
        List[IntervalClock]: The list of schedules

    """
    if isinstance(table_parameters, dict):
        table_parameters = [table_parameters]

    clocks = []
    for count, parameters in enumerate(table_parameters):
        parameter_defaults = parameters | general_flow_params
        clocks.append(
            IntervalClock(
                interval=clock_interval,
                start_date=start_date
                + timedelta(minutes=runs_interval_minutes * count),
                labels=labels,
                parameter_defaults=parameter_defaults,
            )
        )
    return clocks


def query_logs_func(
    dataset_id: str,
    table_id: str,
    datetime_filter=None,
    max_recaptures: int = 60,
    interval_minutes: int = 1,
):
    """
    Queries capture logs to check for errors

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        datetime_filter (pendulum.datetime.DateTime, optional):
        filter passed to query. This task will query the logs table
        for the last 1 day before datetime_filter
        max_recaptures (int, optional): maximum number of recaptures to be done
        interval_minutes (int, optional): interval in minutes between each recapture

    Returns:
        lists: errors (bool),
        timestamps (list of pendulum.datetime.DateTime),
        previous_errors (list of previous errors)
    """

    if not datetime_filter:
        datetime_filter = pendulum.now(constants.TIMEZONE.value).replace(
            second=0, microsecond=0
        )
    elif isinstance(datetime_filter, str):
        datetime_filter = datetime.fromisoformat(datetime_filter).replace(
            second=0, microsecond=0
        )

    datetime_filter = datetime_filter.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
    WITH
    t AS (
    SELECT
        DATETIME(timestamp_array) AS timestamp_array
    FROM
        UNNEST(
            GENERATE_TIMESTAMP_ARRAY(
                TIMESTAMP_SUB('{datetime_filter}', INTERVAL 1 day),
                TIMESTAMP('{datetime_filter}'),
                INTERVAL {interval_minutes} minute) )
        AS timestamp_array
    WHERE
        timestamp_array < '{datetime_filter}' ),
    logs_table AS (
        SELECT
            SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura),
                    "America/Sao_Paulo") AS DATETIME) timestamp_captura,
            SAFE_CAST(sucesso AS BOOLEAN) sucesso,
            SAFE_CAST(erro AS STRING) erro,
            SAFE_CAST(DATA AS DATE) DATA
        FROM
            rj-smtr-staging.{dataset_id}_staging.{table_id}_logs AS t
    ),
    logs AS (
        SELECT
            *,
            TIMESTAMP_TRUNC(timestamp_captura, minute) AS timestamp_array
        FROM
            logs_table
        WHERE
            DATA BETWEEN DATE(DATETIME_SUB('{datetime_filter}',
                            INTERVAL 1 day))
            AND DATE('{datetime_filter}')
            AND timestamp_captura BETWEEN
                DATETIME_SUB('{datetime_filter}', INTERVAL 1 day)
            AND '{datetime_filter}'
        ORDER BY
            timestamp_captura )
    SELECT
        CASE
            WHEN logs.timestamp_captura IS NOT NULL THEN logs.timestamp_captura
        ELSE
            t.timestamp_array
        END
            AS timestamp_captura,
            logs.erro
    FROM
        t
    LEFT JOIN
        logs
    ON
        logs.timestamp_array = t.timestamp_array
    WHERE
        logs.sucesso IS NOT TRUE
    ORDER BY
        timestamp_captura
    """
    log(f"Run query to check logs:\n{query}")
    results = bd.read_sql(query=query, billing_project_id=bq_project())
    if len(results) > 0:
        results["timestamp_captura"] = (
            pd.to_datetime(results["timestamp_captura"])
            .dt.tz_localize(constants.TIMEZONE.value)
            .to_list()
        )
        log(f"Recapture data for the following {len(results)} timestamps:\n{results}")
        if len(results) > max_recaptures:
            message = f"""
            [SPPO - Recaptures]
            Encontradas {len(results)} timestamps para serem recapturadas.
            Essa run processará as seguintes:
            #####
            {results[:max_recaptures]}
            #####
            Sobraram as seguintes para serem recapturadas na próxima run:
            #####
            {results[max_recaptures:]}
            #####
            """
            log_critical(message)
            results = results[:max_recaptures]
        return True, results["timestamp_captura"].to_list(), results["erro"].to_list()
    return False, [], []


def dict_contains_keys(input_dict: dict, keys: list[str]) -> bool:
    """
    Test if the input dict has all keys present in the list

    Args:
        input_dict (dict): the dict to test if has the keys
        keys (list[str]): the list containing the keys to check
    Returns:
        bool: True if the input_dict has all the keys otherwise False
    """
    return all(x in input_dict.keys() for x in keys)


def custom_serialization(obj: Any) -> Any:
    """
    Function to serialize not JSON serializable objects

    Args:
        obj (Any): Object to serialize

    Returns:
        Any: Serialized object
    """
    if isinstance(obj, pd.Timestamp):
        if obj.tzinfo is None:
            obj = obj.tz_localize("UTC").tz_convert(
                emd_constants.DEFAULT_TIMEZONE.value
            )
        # if obj.tzinfo is None:
        #     obj = obj.tz_localize(emd_constants.DEFAULT_TIMEZONE.value)
        # else:
        #     obj = obj.tz_convert(emd_constants.DEFAULT_TIMEZONE.value)

        return obj.isoformat()

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def save_raw_local_func(
    data: Union[dict, str],
    filepath: str,
    mode: str = "raw",
    filetype: str = "json",
) -> str:
    """
    Saves json response from API to .json file.
    Args:
        data (Union[dict, str]): Raw data to save
        filepath (str): Path which to save raw file
        mode (str, optional): Folder to save locally, later folder which to upload to GCS.
        filetype (str, optional): The file format
    Returns:
        str: Path to the saved file
    """

    # diferentes tipos de arquivos para salvar
    _filepath = filepath.format(mode=mode, filetype=filetype)
    Path(_filepath).parent.mkdir(parents=True, exist_ok=True)

    if filetype == "json":
        if isinstance(data, str):
            data = json.loads(data)
        with Path(_filepath).open("w", encoding="utf-8") as fi:
            json.dump(data, fi, default=custom_serialization)

    if filetype in ("txt", "csv"):
        with open(_filepath, "w", encoding="utf-8") as file:
            file.write(data)

    log(f"Raw data saved to: {_filepath}")
    return _filepath


def get_raw_data_api(  # pylint: disable=R0912
    url: str,
    secret_path: str = None,
    api_params: dict = None,
    filetype: str = None,
) -> tuple[str, str, str]:
    """
    Request data from URL API

    Args:
        url (str): URL to request data
        secret_path (str, optional): Secret path to get headers. Defaults to None.
        api_params (dict, optional): Parameters to pass to API. Defaults to None.
        filetype (str, optional): Filetype to save raw file. Defaults to None.

    Returns:
        tuple[str, str, str]: Error, data and filetype
    """
    error = None
    data = None
    try:
        if secret_path is None:
            headers = secret_path
        else:
            headers = get_vault_secret(secret_path)["data"]

        response = requests.get(
            url,
            headers=headers,
            timeout=constants.MAX_TIMEOUT_SECONDS.value,
            params=api_params,
        )

        response.raise_for_status()

        if filetype == "json":
            data = response.json()
        else:
            data = response.text

    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error, data, filetype


def get_upload_storage_blob(
    dataset_id: str,
    filename: str,
) -> Blob:
    """
    Get a blob from upload zone in storage

    Args:
        dataset_id (str): The dataset id on BigQuery.
        filename (str): The filename in GCS.

    Returns:
        Blob: blob object
    """
    bucket = bd.Storage(dataset_id="", table_id="")
    blob_list = list(
        bucket.client["storage_staging"]
        .bucket(bucket.bucket_name)
        .list_blobs(prefix=f"upload/{dataset_id}/{filename}.")
    )
    return blob_list[0]


def get_raw_data_gcs(
    dataset_id: str,
    table_id: str,
    zip_filename: str = None,
) -> tuple[str, str, str]:
    """
    Get raw data from GCS

    Args:
        dataset_id (str): The dataset id on BigQuery.
        table_id (str): The table id on BigQuery.
        zip_filename (str, optional): The zip file name. Defaults to None.

    Returns:
        tuple[str, str, str]: Error, data and filetype
    """
    error = None
    data = None
    filetype = None

    try:
        blob_search_name = zip_filename or table_id
        blob = get_upload_storage_blob(dataset_id=dataset_id, filename=blob_search_name)

        filename = blob.name
        filetype = filename.split(".")[-1]

        data = blob.download_as_bytes()

        if filetype == "zip":
            with zipfile.ZipFile(io.BytesIO(data), "r") as zipped_file:
                filenames = zipped_file.namelist()
                filename = list(
                    filter(lambda x: x.split(".")[0] == table_id, filenames)
                )[0]
                filetype = filename.split(".")[-1]
                data = zipped_file.read(filename)

        data = data.decode(encoding="utf-8")

    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error, data, filetype


def get_raw_data_db(
    query: str, engine: str, host: str, secret_path: str, database: str
) -> tuple[str, str, str]:
    """
    Get data from Databases

    Args:
        query (str): the SQL Query to execute
        engine (str): The datase management system
        host (str): The database host
        secret_path (str): Secret path to get credentials
        database (str): The database to connect

    Returns:
        tuple[str, str, str]: Error, data and filetype
    """
    connector_mapping = {
        "postgresql": psycopg2.connect,
        "mysql": pymysql.connect,
    }

    data = None
    error = None
    filetype = "json"

    try:
        credentials = get_vault_secret(secret_path)["data"]

        with connector_mapping[engine](
            host=host,
            user=credentials["user"],
            password=credentials["password"],
            database=database,
        ) as connection:
            data = pd.read_sql(sql=query, con=connection).to_dict(orient="records")

    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error, data, filetype


def save_treated_local_func(
    filepath: str, data: pd.DataFrame, error: str, mode: str = "staging"
) -> str:
    """
    Save treated file to CSV.

    Args:
        filepath (str): Path to save file
        data (pd.DataFrame): Dataframe to save
        error (str): Error catched during execution
        mode (str, optional): Folder to save locally, later folder which to upload to GCS.

    Returns:
        str: Path to the saved file
    """
    _filepath = filepath.format(mode=mode, filetype="csv")
    Path(_filepath).parent.mkdir(parents=True, exist_ok=True)
    if error is None:
        data.to_csv(_filepath, index=False)
        log(f"Treated data saved to: {_filepath}")
    return _filepath


def upload_run_logs_to_bq(  # pylint: disable=R0913
    dataset_id: str,
    parent_table_id: str,
    timestamp: str,
    error: str = None,
    previous_error: str = None,
    recapture: bool = False,
    mode: str = "raw",
):
    """
    Upload execution status table to BigQuery.
    Table is uploaded to the same dataset, named {parent_table_id}_logs.
    If passing status_dict, should not pass timestamp and error.

    Args:
        dataset_id (str): dataset_id on BigQuery
        parent_table_id (str): table_id on BigQuery
        timestamp (str): timestamp to get datetime range
        error (str): error catched during execution
        previous_error (str): previous error catched during execution
        recapture (bool): if the execution was a recapture
        mode (str): folder to save locally, later folder which to upload to GCS

    Returns:
        None
    """
    table_id = parent_table_id + "_logs"
    # Create partition directory
    filename = f"{table_id}_{timestamp.isoformat()}"
    partition = f"data={timestamp.date()}"
    filepath = Path(
        f"""data/{mode}/{dataset_id}/{table_id}/{partition}/{filename}.csv"""
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


def get_datetime_range(
    timestamp: datetime,
    interval: timedelta,
) -> dict:
    """
    Task to get datetime range in UTC

    Args:
        timestamp (datetime): timestamp to get datetime range
        interval (timedelta): interval to get datetime range

    Returns:
        dict: datetime range
    """

    start = (
        (timestamp - interval)
        .astimezone(tz=pytz.timezone("UTC"))
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    end = timestamp.astimezone(tz=pytz.timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    return {"start": start, "end": end}


def read_raw_data(filepath: str, csv_args: dict = None) -> tuple[str, pd.DataFrame]:
    """
    Read raw data from file

    Args:
        filepath (str): filepath to read
        csv_args (dict): arguments to pass to pandas.read_csv

    Returns:
        tuple[str, pd.DataFrame]: error and data
    """
    error = None
    data = None
    try:
        file_type = filepath.split(".")[-1]

        if file_type == "json":
            data = pd.read_json(filepath)

            # data = json.loads(data)
        elif file_type in ("txt", "csv"):
            if csv_args is None:
                csv_args = {}
            data = pd.read_csv(filepath, **csv_args)
        else:
            error = "Unsupported raw file extension. Supported only: json, csv and txt"

    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error, data
