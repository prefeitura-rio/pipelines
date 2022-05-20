# -*- coding: utf-8 -*-
"""
General utilities for all pipelines.
"""

import logging
from os import getenv, walk
from os.path import join
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

import basedosdados as bd
import hvac
import numpy as np
import pandas as pd
import prefect
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import telegram
from prefect.client import Client
from prefect.engine.state import State
from prefect.run_configs import KubernetesRun

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
    at_code_owners = []
    for code_owner in code_owners:
        if code_owner.startswith("@"):
            at_code_owners.append(code_owner)
        else:
            at_code_owners.append(f"@{code_owner}")
    message = (
        f":man_facepalming: Flow **{flow.name}** has failed."
        + f'\n  - State message: *"{state.message}"*'
        + "\n  - Link to the failed flow: "
        + f"https://prefect.dados.rio/flow-run/{flow_run_id}"
        + "\n  - Extra attention:"
        + "\n    - ".join(at_code_owners)
    )
    send_discord_message(
        message=message,
        webhook_url=url,
    )


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
    if parameters:
        return flow.run(parameters=parameters)
    return flow.run()


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


def dataframe_to_csv(dataframe: pd.DataFrame, path: Union[str, Path]) -> None:
    """
    Writes a dataframe to CSV file.
    """
    # Remove filename from path
    path = Path(path)
    # Create directory if it doesn't exist
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write dataframe to CSV
    dataframe.to_csv(path, index=False, encoding="utf-8")


def dataframe_to_parquet(dataframe: pd.DataFrame, path: Union[str, Path]):
    """
    Writes a dataframe to Parquet file with Schema as STRING.
    """
    # Code adapted from
    # https://stackoverflow.com/a/70817689/9944075

    # Load table from pandas
    table = pa.Table.from_pandas(dataframe)

    # If the file already exists, we:
    # - Load it
    # - Cast `table` to the same schema
    # - Open up a writer
    # - Write the original table
    # - Write the new table
    # - Close the writer
    if Path(path).exists():
        # Load it
        original_table = pq.read_table(
            source=path, pre_buffer=False, use_threads=True, memory_map=True
        )
        # Cast `table` to the same schema
        table = table.cast(original_table.schema)
        # Open up a writer
        writer = pq.ParquetWriter(path, table.schema)
        # Write the original table
        writer.write_table(original_table)
    # If the file doesn't exist, we:
    # - Setup data types
    # - Cast `table` to the schema
    # - Open up a writer
    # - Write the table
    # - Close the writer
    else:
        # Setup data types
        schema = pa.schema([pa.field(col, pa.string()) for col in dataframe.columns])
        # Cast `table` to the schema
        table.cast(target_schema=schema)
        # Open up a writer
        writer = pq.ParquetWriter(path, table.schema)

    # Write the new table
    writer.write_table(table)
    # Close the writer
    writer.close()


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
    return list(
        dataframe.columns.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace(" ", "_")
        .str.replace(".", "")
        .str.replace("/", "_")
        .str.replace("-", "_")
        .str.lower()
    )


def to_partitions(
    data: pd.DataFrame,
    partition_columns: List[str],
    savepath: str,
    data_type: str = "csv",
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
            file_filter_save_path = Path(filter_save_path) / f"data.{data_type}"
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
        raise BaseException("Data need to be a pandas DataFrame")


###############
#
# Storage utils
#
###############


def get_storage_blobs(dataset_id: str, table_id: str) -> list:
    """
    Get all blobs from a table in a dataset.
    """

    storage = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    return list(
        storage.client["storage_staging"]
        .bucket(storage.bucket_name)
        .list_blobs(prefix=f"staging/{storage.dataset_id}/{storage.table_id}/")
    )


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


def dump_header_to_csv(
    data_path: Union[str, Path],
):
    """
    Writes a header to a CSV file.
    """
    # Remove filename from path
    path = Path(data_path)
    if not path.is_dir():
        path = path.parent
    # Grab first CSV file found
    found: bool = False
    file: str = None
    for subdir, _, filenames in walk(str(path)):
        for fname in filenames:
            if fname.endswith(".csv"):
                file = join(subdir, fname)
                log(f"Found CSV file: {file}")
                found = True
                break
        if found:
            break

    save_header_path = f"data/{uuid4()}"
    # discover if it's a partitioned table
    if partition_folders := [folder for folder in file.split("/") if "=" in folder]:
        partition_path = "/".join(partition_folders)
        save_header_file_path = Path(f"{save_header_path}/{partition_path}/header.csv")
        log(f"Found partition path: {save_header_file_path}")

    else:
        save_header_file_path = Path(f"{save_header_path}/header.csv")
        log(f"Do not found partition path: {save_header_file_path}")

    # Create directory if it doesn't exist
    save_header_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Read just first row
    dataframe = pd.read_csv(file, nrows=1)

    # Write dataframe to CSV
    dataframe.to_csv(save_header_file_path, index=False, encoding="utf-8")
    log(f"Wrote header CSV: {save_header_file_path}")

    return save_header_path
