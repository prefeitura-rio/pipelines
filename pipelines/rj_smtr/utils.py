# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
General purpose functions for rj_smtr
"""

from ftplib import FTP
from pathlib import Path

from datetime import timedelta, datetime, date
from typing import List, Union, Any
from functools import partial
import traceback
import io
import json
import zipfile
import time
import pytz
import requests
import basedosdados as bd
from basedosdados import Table, Storage
from basedosdados.upload.datatypes import Datatype
import pandas as pd
from google.cloud.storage.blob import Blob
from google.cloud import bigquery
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


def create_bq_table_schema(
    data_sample_path: Union[str, Path],
) -> list[bigquery.SchemaField]:
    """
    Create the bq schema based on the structure of data_sample_path.

    Args:
        data_sample_path (str, Path): Data sample path to auto complete columns names

    Returns:
        list[bigquery.SchemaField]: The table schema
    """

    data_sample_path = Path(data_sample_path)

    if data_sample_path.is_dir():
        data_sample_path = [
            f
            for f in data_sample_path.glob("**/*")
            if f.is_file() and f.suffix == ".csv"
        ][0]

    columns = Datatype(source_format="csv").header(
        data_sample_path=data_sample_path, csv_delimiter=","
    )

    schema = []
    for col in columns:
        schema.append(
            bigquery.SchemaField(name=col, field_type="STRING", description=None)
        )
    return schema


def create_bq_external_table(table_obj: Table, path: str, bucket_name: str):
    """Creates an BigQuery External table based on sample data

    Args:
        table_obj (Table): BD Table object
        path (str): Table data local path
        bucket_name (str, Optional): The bucket name where the data is located
    """

    Storage(
        dataset_id=table_obj.dataset_id,
        table_id=table_obj.table_id,
        bucket_name=bucket_name,
    ).upload(
        path=path,
        mode="staging",
        if_exists="replace",
    )

    bq_table = bigquery.Table(table_obj.table_full_name["staging"])
    project_name = table_obj.client["bigquery_prod"].project
    table_full_name = table_obj.table_full_name["prod"].replace(
        project_name, f"{project_name}.{bucket_name}", 1
    )
    bq_table.description = f"staging table for `{table_full_name}`"

    bq_table.external_data_configuration = Datatype(
        dataset_id=table_obj.dataset_id,
        table_id=table_obj.table_id,
        schema=create_bq_table_schema(
            data_sample_path=path,
        ),
        mode="staging",
        bucket_name=bucket_name,
        partitioned=True,
        biglake_connection_id=None,
    ).external_config

    table_obj.client["bigquery_staging"].create_table(bq_table)


def create_or_append_table(
    dataset_id: str,
    table_id: str,
    path: str,
    partitions: str = None,
    bucket_name: str = None,
):
    """Conditionally create table or append data to its relative GCS folder.

    Args:
        dataset_id (str): target dataset_id on BigQuery
        table_id (str): target table_id on BigQuery
        path (str): Path to .csv data file
        partitions (str): partition string.
        bucket_name (str, Optional): The bucket name to save the data.
    """
    table_arguments = {"table_id": table_id, "dataset_id": dataset_id}
    if bucket_name is not None:
        table_arguments["bucket_name"] = bucket_name

    tb_obj = Table(**table_arguments)
    dirpath = path.split(partitions)[0]

    if bucket_name is not None:
        create_func = partial(
            create_bq_external_table,
            table_obj=tb_obj,
            path=dirpath,
            bucket_name=bucket_name,
        )

        append_func = partial(
            Storage(
                dataset_id=dataset_id, table_id=table_id, bucket_name=bucket_name
            ).upload,
            path=path,
            mode="staging",
            if_exists="replace",
            partitions=partitions,
        )

    else:
        create_func = partial(
            tb_obj.create,
            path=dirpath,
            if_table_exists="pass",
            if_storage_data_exists="replace",
        )

        append_func = partial(
            tb_obj.append,
            filepath=path,
            if_exists="replace",
            timeout=600,
            partitions=partitions,
        )

    if not tb_obj.table_exists("staging"):
        log("Table does not exist in STAGING, creating table...")
        create_func()
        log("Table created in STAGING")
    else:
        log("Table already exists in STAGING, appending to it...")
        append_func()
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
    if isinstance(obj, (pd.Timestamp, date)):
        if isinstance(obj, pd.Timestamp):
            if obj.tzinfo is None:
                obj = obj.tz_localize("UTC").tz_convert(
                    emd_constants.DEFAULT_TIMEZONE.value
                )
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
        if constants.CONTROLE_FINANCEIRO_DATASET_ID.value in _filepath:
            encoding = "Windows-1252"
        else:
            encoding = "utf-8"

        with open(_filepath, "w", encoding=encoding) as file:
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
    bucket_name: str = None,
) -> Blob:
    """
    Get a blob from upload zone in storage

    Args:
        dataset_id (str): The dataset id on BigQuery.
        filename (str): The filename in GCS.
        bucket_name (str, Optional): The bucket name to get the data.

    Returns:
        Blob: blob object
    """
    bucket_arguments = {"dataset_id": "", "table_id": ""}
    if bucket_name is not None:
        bucket_arguments["bucket_name"] = bucket_name

    bucket = bd.Storage(**bucket_arguments)
    log(f"Filename: {filename}, dataset_id: {dataset_id}")
    blob_list = list(
        bucket.client["storage_staging"]
        .bucket(bucket.bucket_name)
        .list_blobs(prefix=f"upload/{dataset_id}/{filename}.")
    )

    return blob_list[0]


def get_raw_data_gcs(
    dataset_id: str, table_id: str, zip_filename: str = None, bucket_name: str = None
) -> tuple[str, str, str]:
    """
    Get raw data from GCS

    Args:
        dataset_id (str): The dataset id on BigQuery.
        table_id (str): The table id on BigQuery.
        zip_filename (str, optional): The zip file name. Defaults to None.
        bucket_name (str, Optional): The bucket name to get the data.

    Returns:
        tuple[str, str, str]: Error, data and filetype
    """
    error = None
    data = None
    filetype = None

    try:
        blob_search_name = zip_filename or table_id
        blob = get_upload_storage_blob(
            dataset_id=dataset_id, filename=blob_search_name, bucket_name=bucket_name
        )

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


def close_db_connection(connection, engine: str):
    """
    Safely close a database connection

    Args:
        connection: the database connection
        engine (str): The datase management system
    """
    if engine == "postgresql":
        if not connection.closed:
            connection.close()
            log("Database connection closed")
    elif engine == "mysql":
        if connection.open:
            connection.close()
            log("Database connection closed")
    else:
        raise NotImplementedError(f"Engine {engine} not supported")


def execute_db_query(
    engine: str,
    query: str,
    connection,
    connector,
    connection_info: dict,
) -> list[dict]:
    """
    Execute a query if retries

    Args:
        query (str): the SQL Query to execute
        engine (str): The database management system
        connection: The database connection
        connector: The database connector (to do reconnections)
        connection_info (dict): The database connector params (to do reconnections)

    Returns:
        list[dict]: The query results

    """
    retries = 10
    for retry in range(retries):
        try:
            log(f"Executing query:\n{query}")
            data = pd.read_sql(sql=query, con=connection).to_dict(orient="records")
            for d in data:
                for k, v in d.items():
                    if pd.isna(v):
                        d[k] = None
            break
        except Exception as err:
            log(f"[ATTEMPT {retry}]: {err}")
            close_db_connection(connection=connection, engine=engine)
            if retry < retries - 1:
                connection = connector(**connection_info)
            else:
                raise err

    close_db_connection(connection=connection, engine=engine)
    return data


def get_raw_data_db(
    query: str,
    engine: str,
    host: str,
    secret_path: str,
    database: str,
    page_size: int = None,
    max_pages: int = None,
) -> tuple[str, str, str]:
    """
    Get data from Databases

    Args:
        query (str): the SQL Query to execute
        engine (str): The database management system
        host (str): The database host
        secret_path (str): Secret path to get credentials
        database (str): The database to connect
        page_size (int, Optional): The maximum number of rows returned by the paginated query
            if you set a value for this argument, the query will have LIMIT and OFFSET appended to it
        max_pages (int, Optional): The maximum number of paginated queries to execute

    Returns:
        tuple[str, str, str]: Error, data and filetype
    """
    connector_mapping = {
        "postgresql": psycopg2.connect,
        "mysql": pymysql.connect,
    }

    data = None
    error = None

    if max_pages is None:
        max_pages = 1

    full_data = []
    credentials = get_vault_secret(secret_path)["data"]

    connector = connector_mapping[engine]

    try:
        connection_info = {
            "host": host,
            "user": credentials["user"],
            "password": credentials["password"],
            "database": database,
        }
        connection = connector(**connection_info)

        for page in range(max_pages):
            if page_size is not None:
                paginated_query = (
                    query + f" LIMIT {page_size} OFFSET {page * page_size}"
                )
            else:
                paginated_query = query

            data = execute_db_query(
                engine=engine,
                query=paginated_query,
                connection=connection,
                connector=connector,
                connection_info=connection_info,
            )

            full_data += data

            log(f"Returned {len(data)} rows")

            if page_size is None or len(data) < page_size:
                log("Database Extraction Finished")
                break

    except Exception:
        full_data = []
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error, full_data, "json"


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
        data.to_csv(
            _filepath,
            index=False,
        )
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
    bucket_name: str = None,
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
        bucket_name (str, Optional): The bucket name to save the data.

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
        log(f"Recapturing {timestamp} with previous error:\n{previous_error}")
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
        bucket_name=bucket_name,
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


def read_raw_data(filepath: str, reader_args: dict = None) -> tuple[str, pd.DataFrame]:
    """
    Read raw data from file

    Args:
        filepath (str): filepath to read
        reader_args (dict): arguments to pass to pandas.read_csv or read_json

    Returns:
        tuple[str, pd.DataFrame]: error and data
    """
    error = None
    data = None
    if reader_args is None:
        reader_args = {}
    try:
        file_type = filepath.split(".")[-1]

        if file_type == "json":
            data = pd.read_json(filepath, **reader_args)

            # data = json.loads(data)
        elif file_type in ("txt", "csv"):
            data = pd.read_csv(filepath, **reader_args)
        else:
            error = "Unsupported raw file extension. Supported only: json, csv and txt"

    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error, data


def get_raw_recursos(
    request_url: str,
    request_params: dict,
) -> tuple[str, str, str]:
    """
    Returns a dataframe with recursos data from movidesk api.
    """
    all_records = False
    top = 1000
    skip = 0
    error = None
    filetype = "json"
    data = []

    while not all_records:
        try:
            request_params["$top"] = top
            request_params["$skip"] = skip

            log(f'top: {request_params["$top"]}, skip: {request_params["$skip"]}')

            log(f"Request url {request_url}")

            MAX_RETRIES = 3

            for retry in range(MAX_RETRIES):
                response = requests.get(
                    request_url,
                    params=request_params,
                    timeout=constants.MAX_TIMEOUT_SECONDS.value,
                )
                if response.ok:
                    break
                elif response.status_code >= 500:
                    log(f"Server error {response.status_code}")
                    if retry == MAX_RETRIES - 1:
                        response.raise_for_status()
                    time.sleep(60)

                else:
                    response.raise_for_status()

            paginated_data = response.json()

            if isinstance(paginated_data, dict):
                paginated_data = [paginated_data]

            if len(paginated_data) == top:
                skip += top
                time.sleep(60)
            else:
                if len(paginated_data) == 0:
                    log("Nenhum dado para tratar.")

                all_records = True
            data += paginated_data

            log(f"Dados (paginados): {len(data)}")

        except Exception as error:
            error = traceback.format_exc()
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")
            data = []
            break

    log(f"Request concluído, tamanho dos dados: {len(data)}.")

    return error, data, filetype


def perform_check(desc: str, check_params: dict, request_params: dict) -> dict:
    """
    Perform a check on a query

    Args:
        desc (str): The check description
        check_params (dict): The check parameters
            * query (str): SQL query to be executed
            * order_columns (list): order columns for query log results, in case of failure (optional)
        request_params (dict): The request parameters

    Returns:
        dict: The check status
    """
    try:
        q = check_params["query"].format(**request_params)
        order_columns = check_params.get("order_columns", None)
    except KeyError as e:
        raise ValueError(f"Missing key in check_params: {e}") from e

    log(q)
    df = bd.read_sql(q)

    check_status = df.empty

    check_status_dict = {"desc": desc, "status": check_status}

    log(f"Check status:\n{check_status_dict}")

    if not check_status:
        log(f"Data info:\n{data_info_str(df)}")
        log(
            f"Sorted data:\n{df.sort_values(by=order_columns) if order_columns else df}"
        )

    return check_status_dict


def perform_checks_for_table(
    table_id: str, request_params: dict, test_check_list: dict, check_params: dict
) -> dict:
    """
    Perform checks for a table

    Args:
        table_id (str): The table id
        request_params (dict): The request parameters
        test_check_list (dict): The test check list
        check_params (dict): The check parameters

    Returns:
        dict: The checks
    """
    request_params["table_id"] = table_id
    checks = list()

    for description, test_check in test_check_list.items():
        request_params["expression"] = test_check.get("expression", "")
        checks.append(
            perform_check(
                description,
                check_params.get(test_check.get("test", "expression_is_true")),
                request_params | test_check.get("params", {}),
            )
        )

    return checks


def format_send_discord_message(formatted_messages: list, webhook_url: str):
    """
    Format and send a message to discord

    Args:
        formatted_messages (list): The formatted messages
        webhook_url (str): The webhook url

    Returns:
        None
    """
    formatted_message = "".join(formatted_messages)
    log(formatted_message)
    msg_ext = len(formatted_message)
    if msg_ext > 2000:
        log(
            f"** Message too long ({msg_ext} characters), will be split into multiple messages **"
        )
        # Split message into lines
        lines = formatted_message.split("\n")
        message_chunks = []
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:  # +1 for the newline character
                message_chunks.append(chunk)
                chunk = ""
            chunk += line + "\n"
        message_chunks.append(chunk)  # Append the last chunk
        for chunk in message_chunks:
            send_discord_message(
                message=chunk,
                webhook_url=webhook_url,
            )
    else:
        send_discord_message(
            message=formatted_message,
            webhook_url=webhook_url,
        )
