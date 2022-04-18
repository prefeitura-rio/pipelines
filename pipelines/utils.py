"""
General utilities for all pipelines.
"""

import logging
from os import getenv
from typing import Any, Dict, Tuple

import hvac
import prefect
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


def query_to_line(query: str) -> str:
    """
    Converts a query to a line.
    """
    return " ".join(query.split("\n"))


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


def untuple_clocks(clocks):
    """
    Converts a list of tuples to a list of clocks.
    """
    return [
        clock[0] if isinstance(clock, tuple) else clock for clock in clocks
    ]
