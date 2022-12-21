# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
General utilities for all pipelines.
"""

import base64
from datetime import datetime
import json
import logging
from os import getenv, walk
from os.path import join
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

import basedosdados as bd
import croniter
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.oauth2 import service_account
import hvac
import numpy as np
import pandas as pd
import prefect
from prefect.client import Client
from prefect.engine.state import Skipped, State
from prefect.run_configs import KubernetesRun
from prefect.utilities.graphql import (
    with_args,
)
from redis_pal import RedisPal
import requests
import telegram

from pipelines.constants import constants


def log(msg: Any, level: str = "info") -> None:
    """
    Logs a message to prefect's logger.
    """
    levels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    blank_spaces = 8 * " "
    msg = blank_spaces + "----\n" + str(msg)
    msg = "\n".join([blank_spaces + line for line in msg.split("\n")]) + "\n\n"

    if level not in levels:
        raise ValueError(f"Invalid log level: {level}")
    prefect.context.logger.log(levels[level], msg)  # pylint: disable=E1101


def log_mod(msg: str, index: int, mod: int):
    """
    Only logs a message if the index is a multiple of mod.
    """
    if index % mod == 0 or index == 1:
        log(msg)


###############
#
# Datetime utils
#
###############


def determine_whether_to_execute_or_not(
    cron_expression: str, datetime_now: datetime, datetime_last_execution: datetime
) -> bool:
    """
    Determines whether the cron expression is currently valid.

    Args:
        cron_expression: The cron expression to check.
        datetime_now: The current datetime.
        datetime_last_execution: The last datetime the cron expression was executed.

    Returns:
        True if the cron expression should trigger, False otherwise.
    """
    cron_expression_iterator = croniter.croniter(
        cron_expression, datetime_last_execution
    )
    next_cron_expression_time = cron_expression_iterator.get_next(datetime)
    return next_cron_expression_time <= datetime_now


###############
#
# Redis
#
###############


def get_redis_client(
    host: str = "redis.redis.svc.cluster.local",
    port: int = 6379,
    db: int = 0,  # pylint: disable=C0103
    password: str = None,
) -> RedisPal:
    """
    Returns a Redis client.
    """
    return RedisPal(
        host=host,
        port=port,
        db=db,
        password=password,
    )


def get_vault_client() -> hvac.Client:
    """
    Returns a Vault client.
    """
    return hvac.Client(
        url=getenv("VAULT_ADDRESS").strip(),
        token=getenv("VAULT_TOKEN").strip(),
    )


def get_vault_secret(secret_path: str, client: hvac.Client = None) -> dict:
    """
    Returns a secret from Vault.
    """
    vault_client = client or get_vault_client()
    return vault_client.secrets.kv.read_secret_version(secret_path)["data"]


def get_username_and_password_from_secret(
    secret_path: str,
    client: hvac.Client = None,
) -> Tuple[str, str]:
    """
    Returns a username and password from a secret in Vault.
    """
    secret = get_vault_secret(secret_path, client)
    return (
        secret["data"]["username"],
        secret["data"]["password"],
    )


def notify_discord_on_failure(
    flow: prefect.Flow,
    state: State,
    secret_path: str,
    code_owners: Optional[List[str]] = None,
):
    """
    Notifies a Discord channel when a flow fails.
    """
    url = get_vault_secret(secret_path)["data"]["url"]
    flow_run_id = prefect.context.get("flow_run_id")
    code_owners = code_owners or constants.DEFAULT_CODE_OWNERS.value
    code_owner_dict = constants.OWNERS_DISCORD_MENTIONS.value
    at_code_owners = []
    for code_owner in code_owners:
        code_owner_id = code_owner_dict[code_owner]["user_id"]
        code_owner_type = code_owner_dict[code_owner]["type"]

        if code_owner_type == "user":
            at_code_owners.append(f"    - <@{code_owner_id}>\n")
        elif code_owner_type == "user_nickname":
            at_code_owners.append(f"    - <@!{code_owner_id}>\n")
        elif code_owner_type == "channel":
            at_code_owners.append(f"    - <#{code_owner_id}>\n")
        elif code_owner_type == "role":
            at_code_owners.append(f"    - <@&{code_owner_id}>\n")

    message = (
        f":man_facepalming: Flow **{flow.name}** has failed."
        + f'\n  - State message: *"{state.message}"*'
        + "\n  - Link to the failed flow: "
        + f"https://prefect.dados.rio/flow-run/{flow_run_id}"
        + "\n  - Extra attention:\n"
        + "".join(at_code_owners)
    )
    send_discord_message(
        message=message,
        webhook_url=url,
    )


# pylint: disable=unused-argument
def skip_if_running_handler(obj, old_state: State, new_state: State) -> State:
    """
    State handler that will skip a flow run if another instance of the flow is already running.

    Adapted from Prefect Discourse:
    https://tinyurl.com/4hn5uz2w
    """
    if new_state.is_running():
        client = Client()
        query = """
            query($flow_id: uuid) {
                flow_run(
                    where: {
                        _and: [
                            {state: {_eq: "Running"}},
                            {flow_id: {_eq: $flow_id}}
                        ]
                    }
                ) {
                    id
                }
            }
        """
        # pylint: disable=no-member
        response = client.graphql(
            query=query,
            variables=dict(flow_id=prefect.context.flow_id),
        )
        active_flow_runs = response["data"]["flow_run"]
        if active_flow_runs:
            logger = prefect.context.get("logger")
            message = "Skipping this flow run since there are already some flow runs in progress"
            logger.info(message)
            return Skipped(message)
    return new_state


def set_default_parameters(
    flow: prefect.Flow, default_parameters: dict
) -> prefect.Flow:
    """
    Sets default parameters for a flow.
    """
    for parameter in flow.parameters():
        if parameter.name in default_parameters:
            parameter.default = default_parameters[parameter.name]
    return flow


def run_local(flow: prefect.Flow, parameters: Dict[str, Any] = None):
    """
    Runs a flow locally.
    """
    # Setup for local run
    flow.storage = None
    flow.run_config = None
    flow.schedule = None

    # Run flow
    return flow.run(parameters=parameters) if parameters else flow.run()


def run_cloud(
    flow: prefect.Flow,
    labels: List[str],
    parameters: Dict[str, Any] = None,
    run_description: str = "",
):
    """
    Runs a flow on Prefect Server (must have VPN configured).
    """
    # Setup no schedule
    flow.schedule = None

    # Change flow name for development and register
    flow.name = f"{flow.name} (development)"
    flow.run_config = KubernetesRun(image="ghcr.io/prefeitura-rio/prefect-flows:latest")
    flow_id = flow.register(project_name="main", labels=[])

    # Get Prefect Client and submit flow run
    client = Client()
    flow_run_id = client.create_flow_run(
        flow_id=flow_id,
        run_name=f"TEST RUN - {run_description} - {flow.name}",
        labels=labels,
        parameters=parameters,
    )

    # Print flow run link so user can check it
    print(f"Run submitted: TEST RUN - {run_description} - {flow.name}")
    print(f"Please check at: https://prefect.dados.rio/flow-run/{flow_run_id}")


def run_registered(
    flow_name: str,
    flow_project: str,
    labels: List[str],
    parameters: Dict[str, Any] = None,
    run_description: str = "",
) -> str:
    """
    Runs an already registered flow on Prefect Server (must have credentials configured).
    """
    # Get Prefect Client and submit flow run
    client = Client()
    flow_result = client.graphql(
        {
            "query": {
                with_args(
                    "flow",
                    {
                        "where": {
                            "_and": [
                                {"name": {"_eq": flow_name}},
                                {"archived": {"_eq": False}},
                                {"project": {"name": {"_eq": flow_project}}},
                            ]
                        }
                    },
                ): {
                    "id",
                }
            }
        }
    )
    flows_found = flow_result.data.flow
    if len(flows_found) == 0:
        raise ValueError(f"Flow {flow_name} not found.")
    if len(flows_found) > 1:
        raise ValueError(f"More than one flow found for {flow_name}.")
    flow_id = flow_result["data"]["flow"][0]["id"]
    flow_run_id = client.create_flow_run(
        flow_id=flow_id,
        run_name=f"SUBMITTED REMOTELY - {run_description}",
        labels=labels,
        parameters=parameters,
    )

    # Print flow run link so user can check it
    print(f"Run submitted: SUBMITTED REMOTELY - {run_description}")
    print(f"Please check at: https://prefect.dados.rio/flow-run/{flow_run_id}")

    return flow_run_id


def query_to_line(query: str) -> str:
    """
    Converts a query to a line.
    """
    return " ".join([line.strip() for line in query.split("\n")])


def send_discord_message(
    message: str,
    webhook_url: str,
) -> None:
    """
    Sends a message to a Discord channel.
    """
    requests.post(
        webhook_url,
        data={"content": message},
    )


def send_telegram_message(
    message: str,
    token: str,
    chat_id: int,
    parse_mode: str = telegram.ParseMode.HTML,
):
    """
    Sends a message to a Telegram chat.
    """
    bot = telegram.Bot(token=token)
    bot.send_message(
        chat_id=chat_id,
        text=message,
        parse_mode=parse_mode,
    )


def smart_split(
    text: str,
    max_length: int,
    separator: str = " ",
) -> List[str]:
    """
    Splits a string into a list of strings.
    """
    if len(text) <= max_length:
        return [text]

    separator_index = text.rfind(separator, 0, max_length)
    if (separator_index >= max_length) or (separator_index == -1):
        raise ValueError(
            f'Cannot split text "{text}" into {max_length}'
            f'characters using separator "{separator}"'
        )

    return [
        text[:separator_index],
        *smart_split(
            text[separator_index + len(separator) :],
            max_length,
            separator,
        ),
    ]


def untuple_clocks(clocks):
    """
    Converts a list of tuples to a list of clocks.
    """
    return [clock[0] if isinstance(clock, tuple) else clock for clock in clocks]


###############
#
# Text formatting
#
###############


def human_readable(
    value: Union[int, float],
    unit: str = "",
    unit_prefixes: List[str] = None,
    unit_divider: int = 1000,
    decimal_places: int = 2,
):
    """
    Formats a value in a human readable way.
    """
    if unit_prefixes is None:
        unit_prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"]
    if value == 0:
        return f"{value}{unit}"
    unit_prefix = unit_prefixes[0]
    for prefix in unit_prefixes[1:]:
        if value < unit_divider:
            break
        unit_prefix = prefix
        value /= unit_divider
    return f"{value:.{decimal_places}f}{unit_prefix}{unit}"


###############
#
# Dataframe
#
###############


def dataframe_to_csv(
    dataframe: pd.DataFrame,
    path: Union[str, Path],
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
) -> None:
    """
    Writes a dataframe to CSV file.
    """
    if build_json_dataframe:
        dataframe = to_json_dataframe(dataframe, key_column=dataframe_key_column)

    # Remove filename from path
    path = Path(path)
    # Create directory if it doesn't exist
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write dataframe to CSV
    dataframe.to_csv(path, index=False, encoding="utf-8")


def dataframe_to_parquet(
    dataframe: pd.DataFrame,
    path: Union[str, Path],
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
):
    """
    Writes a dataframe to Parquet file with Schema as STRING.
    """
    # Code adapted from
    # https://stackoverflow.com/a/70817689/9944075

    if build_json_dataframe:
        dataframe = to_json_dataframe(dataframe, key_column=dataframe_key_column)

    # If the file already exists, we:
    # - Load it
    # - Merge the new dataframe with the existing one
    if Path(path).exists():
        # Load it
        original_df = pd.read_parquet(path)
        # Merge the new dataframe with the existing one
        dataframe = pd.concat([original_df, dataframe], sort=False)

    # Write dataframe to Parquet
    dataframe.to_parquet(path, engine="pyarrow")


def batch_to_dataframe(batch: Tuple[Tuple], columns: List[str]) -> pd.DataFrame:
    """
    Converts a batch of rows to a dataframe.
    """
    return pd.DataFrame(batch, columns=columns)


def clean_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans a dataframe.
    """
    for col in dataframe.columns.tolist():
        if dataframe[col].dtype == object:
            try:
                dataframe[col] = (
                    dataframe[col]
                    .astype(str)
                    .str.replace("\x00", "")
                    .replace("None", np.nan)
                )
            except Exception as exc:
                print(
                    "Column: ",
                    col,
                    "\nData: ",
                    dataframe[col].tolist(),
                    "\n",
                    exc,
                )
                raise
    return dataframe


def remove_columns_accents(dataframe: pd.DataFrame) -> list:
    """
    Remove accents from dataframe columns.
    """
    columns = [str(column) for column in dataframe.columns]
    dataframe.columns = columns
    return list(
        dataframe.columns.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .map(lambda x: x.strip())
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("-", "_")
        .str.replace("\a", "_")
        .str.replace("\b", "_")
        .str.replace("\n", "_")
        .str.replace("\t", "_")
        .str.replace("\v", "_")
        .str.replace("\f", "_")
        .str.replace("\r", "_")
        .str.lower()
        .map(final_column_treatment)
    )


# pylint: disable=R0913
def to_partitions(
    data: pd.DataFrame,
    partition_columns: List[str],
    savepath: str,
    data_type: str = "csv",
    suffix: str = None,
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
):  # sourcery skip: raise-specific-error
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """

    if isinstance(data, (pd.core.frame.DataFrame)):

        savepath = Path(savepath)

        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns).reset_index(drop=True)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            if suffix is not None:
                file_filter_save_path = (
                    Path(filter_save_path) / f"data_{suffix}.{data_type}"
                )
            else:
                file_filter_save_path = Path(filter_save_path) / f"data.{data_type}"

            if build_json_dataframe:
                df_filter = to_json_dataframe(
                    df_filter, key_column=dataframe_key_column
                )

            if data_type == "csv":
                # append data to csv
                df_filter.to_csv(
                    file_filter_save_path,
                    index=False,
                    mode="a",
                    header=not file_filter_save_path.exists(),
                )
            elif data_type == "parquet":
                dataframe_to_parquet(dataframe=df_filter, path=file_filter_save_path)
            else:
                raise ValueError(f"Invalid data type: {data_type}")
    else:
        raise BaseException("Data need to be a pandas DataFrame")


def to_json_dataframe(
    dataframe: pd.DataFrame = None,
    csv_path: Union[str, Path] = None,
    key_column: str = None,
    read_csv_kwargs: dict = None,
    save_to: Union[str, Path] = None,
) -> pd.DataFrame:
    """
    Manipulates a dataframe by keeping key_column and moving every other column
    data to a "content" column in JSON format. Example:

    - Input dataframe: pd.DataFrame({"key": ["a", "b", "c"], "col1": [1, 2, 3], "col2": [4, 5, 6]})
    - Output dataframe: pd.DataFrame({
        "key": ["a", "b", "c"],
        "content": [{"col1": 1, "col2": 4}, {"col1": 2, "col2": 5}, {"col1": 3, "col2": 6}]
    })
    """
    if dataframe is None and not csv_path:
        raise ValueError("dataframe or dataframe_path is required")
    if csv_path:
        dataframe = pd.read_csv(csv_path, **read_csv_kwargs)
    if key_column:
        dataframe["content"] = dataframe.drop(columns=[key_column]).to_dict(
            orient="records"
        )
        dataframe = dataframe[["key", "content"]]
    else:
        dataframe["content"] = dataframe.to_dict(orient="records")
        dataframe = dataframe[["content"]]
    if save_to:
        dataframe.to_csv(save_to, index=False)
    return dataframe


###############
#
# Storage utils
#
###############


def get_credentials_from_env(
    mode: str = "prod", scopes: List[str] = None
) -> service_account.Credentials:
    """
    Gets credentials from env vars
    """
    if mode not in ["prod", "staging"]:
        raise ValueError("Mode must be 'prod' or 'staging'")
    env: str = getenv(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()}", "")
    if env == "":
        raise ValueError(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()} env var not set!")
    info: dict = json.loads(base64.b64decode(env))
    cred: service_account.Credentials = (
        service_account.Credentials.from_service_account_info(info)
    )
    if scopes:
        cred = cred.with_scopes(scopes)
    return cred


def get_storage_blobs(dataset_id: str, table_id: str) -> list:
    """
    Get all blobs from a table in a dataset.
    """

    bd_storage = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    return list(
        bd_storage.client["storage_staging"]
        .bucket(bd_storage.bucket_name)
        .list_blobs(prefix=f"staging/{bd_storage.dataset_id}/{bd_storage.table_id}/")
    )


def list_blobs_with_prefix(
    bucket_name: str, prefix: str, mode: str = "prod"
) -> List[Blob]:
    """
    Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    Mode needs to be "prod" or "staging"
    """

    credentials = get_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    return list(blobs)


def upload_files_to_storage(
    bucket_name: str,
    prefix: str,
    local_path: Union[str, Path] = None,
    files_list: List[str] = None,
    mode: str = "prod",
):
    """
    Uploads all files from `local_path` to `bucket_name` with `prefix`.
    Mode needs to be "prod" or "staging"
    """
    # Either local_path or files_list must be provided
    if local_path is None and files_list is None:
        raise ValueError("Either local_path or files_list must be provided")

    # If local_path is provided, get all files from it
    if local_path is not None:
        files_list: List[Path] = list(Path(local_path).glob("**/*"))

    # Assert all items in files_list are Path objects
    files_list: List[Path] = [Path(f) for f in files_list]

    credentials = get_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    bucket = storage_client.bucket(bucket_name)

    for file in files_list:
        if file.is_file():
            blob = bucket.blob(f"{prefix}/{file.name}")
            blob.upload_from_filename(file)


def is_date(date_string: str, date_format: str = "%Y-%m-%d") -> Union[datetime, bool]:
    """
    Checks whether a string is a valid date.
    """
    try:
        return datetime.strptime(date_string, date_format).strftime(date_format)
    except ValueError:
        return False


def parser_blobs_to_partition_dict(blobs: list) -> dict:
    """
    Extracts the partition information from the blobs.
    """

    partitions_dict = {}
    for blob in blobs:
        for folder in blob.name.split("/"):
            if "=" in folder:
                key = folder.split("=")[0]
                value = folder.split("=")[1]
                try:
                    partitions_dict[key].append(value)
                except KeyError:
                    partitions_dict[key] = [value]
    return partitions_dict


def dump_header_to_file(data_path: Union[str, Path], data_type: str = "csv"):
    """
    Writes a header to a CSV file.
    """
    try:
        assert data_type in ["csv", "parquet"]
    except AssertionError as exc:
        raise ValueError(f"Invalid data type: {data_type}") from exc
    # Remove filename from path
    path = Path(data_path)
    if not path.is_dir():
        path = path.parent
    # Grab first `data_type` file found
    found: bool = False
    file: str = None
    for subdir, _, filenames in walk(str(path)):
        for fname in filenames:
            if fname.endswith(f".{data_type}"):
                file = join(subdir, fname)
                log(f"Found {data_type.upper()} file: {file}")
                found = True
                break
        if found:
            break

    save_header_path = f"data/{uuid4()}"
    # discover if it's a partitioned table
    if partition_folders := [folder for folder in file.split("/") if "=" in folder]:
        partition_path = "/".join(partition_folders)
        save_header_file_path = Path(
            f"{save_header_path}/{partition_path}/header.{data_type}"
        )
        log(f"Found partition path: {save_header_file_path}")

    else:
        save_header_file_path = Path(f"{save_header_path}/header.{data_type}")
        log(f"Do not found partition path: {save_header_file_path}")

    # Create directory if it doesn't exist
    save_header_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Read just first row and write dataframe to file
    if data_type == "csv":
        dataframe = pd.read_csv(file, nrows=1)
        dataframe.to_csv(save_header_file_path, index=False, encoding="utf-8")
    elif data_type == "parquet":
        dataframe = pd.read_parquet(file)[:1]
        dataframe_to_parquet(dataframe=dataframe, path=save_header_file_path)

    log(f"Wrote {data_type.upper()} header at {save_header_file_path}")

    return save_header_path


def parse_date_columns(
    dataframe: pd.DataFrame, partition_date_column: str
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Parses the date columns to the partition format.
    """
    ano_col = "ano_particao"
    mes_col = "mes_particao"
    data_col = "data_particao"
    cols = [ano_col, mes_col, data_col]
    for col in cols:
        if col in dataframe.columns:
            raise ValueError(f"Column {col} already exists, please review your model.")

    dataframe[partition_date_column] = dataframe[partition_date_column].astype(str)
    dataframe[data_col] = pd.to_datetime(
        dataframe[partition_date_column], errors="coerce"
    )

    dataframe[ano_col] = (
        dataframe[data_col]
        .dt.year.fillna(-1)
        .astype(int)
        .astype(str)
        .replace("-1", np.nan)
    )

    dataframe[mes_col] = (
        dataframe[data_col]
        .dt.month.fillna(-1)
        .astype(int)
        .astype(str)
        .replace("-1", np.nan)
    )

    dataframe[data_col] = dataframe[data_col].dt.date

    return dataframe, [ano_col, mes_col, data_col]


def final_column_treatment(column: str) -> str:
    """
    Adds an underline before column name if it only has numbers or remove all non alpha numeric
    characters besides underlines ("_").
    """
    try:
        int(column)
        return f"_{column}"
    except ValueError:  # pylint: disable=bare-except
        non_alpha_removed = re.sub(r"[\W]+", "", column)
        return non_alpha_removed
