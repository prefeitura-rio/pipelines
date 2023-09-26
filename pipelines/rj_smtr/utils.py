# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
General purpose functions for rj_smtr
"""

from ftplib import FTP
from pathlib import Path

from datetime import timedelta, datetime
from typing import List
import io
import basedosdados as bd
from basedosdados import Table
import pandas as pd
import pytz
import requests
import zipfile

from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants as emd_constants

from pipelines.rj_smtr.implicit_ftp import ImplicitFtpTls
from pipelines.rj_smtr.constants import constants

from pipelines.utils.utils import (
    log,
    get_vault_secret,
    send_discord_message,
    get_redis_client,
    get_storage_blobs,
    get_storage_blob,
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
    interval: timedelta,
    labels: List[str],
    table_parameters: list,
    dataset_id: str,
    secret_path: str,
    runs_interval_minutes: int = 15,
    start_date: datetime = datetime(
        2020, 1, 1, tzinfo=pytz.timezone(emd_constants.DEFAULT_TIMEZONE.value)
    ),
) -> List[IntervalClock]:
    """
    Generates multiple schedules

    Args:
        interval (timedelta): The interval to run the schedule
        labels (List[str]): The labels to be added to the schedule
        table_parameters (list): The table parameters
        dataset_id (str): The dataset_id to be used in the schedule
        secret_path (str): The secret path to be used in the schedule
        runs_interval_minutes (int, optional): The interval between each schedule. Defaults to 15.
        start_date (datetime, optional): The start date of the schedule.
            Defaults to datetime(2020, 1, 1, tzinfo=pytz.timezone(emd_constants.DEFAULT_TIMEZONE.value)).

    Returns:
        List[IntervalClock]: The list of schedules

    """

    clocks = []
    for count, parameters in enumerate(table_parameters):
        parameter_defaults = {
            "table_params": parameters,
            "dataset_id": dataset_id,
            "secret_path": secret_path,
            "interval": interval.total_seconds(),
        }
        log(f"parameter_defaults: {parameter_defaults}")
        clocks.append(
            IntervalClock(
                interval=interval,
                start_date=start_date
                + timedelta(minutes=runs_interval_minutes * count),
                labels=labels,
                parameter_defaults=parameter_defaults,
            )
        )
    return clocks


def get_raw_data_api(  # pylint: disable=R0912
    url: str,
    headers: str = None,
    filetype: str = "json",
    csv_args: dict = None,
    params: dict = None,
) -> list[dict]:
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

    except Exception as exp:
        error = exp

    if error is not None:
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": data, "error": error}


def get_raw_data_gcs(
    dataset_id: str, table_id: str, file_name: str, mode: str, zip_file_name: str = None
) -> dict:
    error = None
    data = None
    try:
        if zip_file_name:
            blob = get_storage_blob(
                dataset_id=dataset_id,
                table_id=table_id,
                file_name=zip_file_name,
                mode=mode,
            )
            compressed_data = blob.download_as_bytes()
            with zipfile.ZipFile(io.BytesIO(compressed_data), "r") as zipped_file:
                data = zipped_file.read(file_name).decode(encoding="utf-8")
        else:
            blob = get_storage_blob(
                dataset_id=dataset_id, table_id=table_id, file_name=file_name, mode=mode
            )
            data = blob.download_as_string()
    except Exception as exp:
        error = exp

    return {"data": data, "error": error}
