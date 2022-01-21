"""
General utilities for all pipelines.
"""

import logging
from os import getenv
from typing import Any, Tuple

import hvac
import prefect


def log(msg: Any, level: str = 'info') -> None:
    """
    Logs a message to prefect's logger.
    """
    levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL,
    }
    if level not in levels:
        raise ValueError(f"Invalid log level: {level}")
    prefect.context.logger.log(levels[level], msg)  # pylint: disable=E1101


def get_vault_client() -> hvac.Client:
    """
    Returns a Vault client.
    """
    return hvac.Client(url=getenv('VAULT_ADDRESS'), token=getenv('VAULT_TOKEN'))


def get_vault_secret(secret_path: str, client: hvac.Client = None) -> dict:
    """
    Returns a secret from Vault.
    """
    vault_client = client if client else get_vault_client()
    return vault_client.secrets.kv.read_secret_version(secret_path)['data']


def get_username_and_password_from_secret(
    secret_path: str,
    client: hvac.Client = None,
) -> Tuple[str, str]:
    """
    Returns a username and password from a secret in Vault.
    """
    secret = get_vault_secret(secret_path, client)
    return secret['username'], secret['password']
