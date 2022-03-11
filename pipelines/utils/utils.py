"""
General utilities for all pipelines.
"""

import logging
from os import getenv
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import hvac
import numpy as np
import pandas as pd
import prefect
from prefect.client import Client
from prefect.engine.state import State
from prefect.run_configs import KubernetesRun
import requests
import telegram


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
    if level not in levels:
        raise ValueError(f"Invalid log level: {level}")
    prefect.context.logger.log(levels[level], msg)  # pylint: disable=E1101


@prefect.task(checkpoint=False)
def log_task(msg: Any, level: str = "info"):
    """
    Logs a message to prefect's logger.
    """
    log(msg, level)


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
    vault_client = client if client else get_vault_client()
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
):
    """
    Notifies a Discord channel when a flow fails.
    """
    url = get_vault_secret(secret_path)["data"]["url"]
    flow_run_id = prefect.context.get("flow_run_id")
    message = (
        f":man_facepalming: Flow **{flow.name}** has failed."
        + f'\n  - State message: *"{state.message}"*'
        + "\n  - Link to the failed flow: "
        + f"http://prefect-ui.prefect.svc.cluster.local:8080/flow-run/{flow_run_id}"
    )
    send_discord_message(
        message=message,
        webhook_url=url,
    )


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


def run_cloud(flow: prefect.Flow, labels: List[str], parameters: Dict[str, Any] = None):
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
        run_name=f"TEST RUN - {flow.name}",
        labels=labels,
        parameters=parameters,
    )

    # Print flow run link so user can check it
    print("Run submitted, please check it at:")
    print(f"http://prefect-ui.prefect.svc.cluster.local:8080/flow-run/{flow_run_id}")


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
# Dataframe
#
###############


def dataframe_to_csv(dataframe: pd.DataFrame, path: Union[str, Path]) -> None:
    """
    Writes a dataframe to a CSV file.
    """
    # Remove filename from path
    path = Path(path)
    # Create directory if it doesn't exist
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write dataframe to CSV
    log(f"Writing dataframe to CSV: {path}")
    dataframe.to_csv(path, index=False, encoding="utf-8")
    log(f"Wrote dataframe to CSV: {path}")


def batch_to_dataframe(batch: Tuple[Tuple], columns: List[str]) -> pd.DataFrame:
    """
    Converts a batch of rows to a dataframe.
    """
    log(f"Converting batch of size {len(batch)} to dataframe")
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
                    .str.replace("\x00", "", regex=True)
                    .replace("None", np.nan, regex=True)
                )
            except Exception as exc:
                print("Column: ", col, "\nData: ", dataframe[col].tolist(), "\n", exc)
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
    )


###############
#
# File
#
###############
