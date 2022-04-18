# -*- coding: utf-8 -*-
"""
Tasks for sending messages through a Whatsapp Bot.
This module assumes that the bot inherits from the template bot at
https://github.com/prefeitura-rio/whatsapp-bot
"""

from prefect import task
import requests
from pipelines.utils.utils import get_vault_secret

from pipelines.utils.whatsapp_bot.exceptions import (
    WhatsAppBotNotFoundError,
    WhatsAppSendMessageError,
)


@task
def get_whatsapp_bot_api_url(secret_path: str) -> str:
    """
    Get the URL of the bot API.

    Args:
        secret_path (str): Path to the secret on Vault.

    Returns:
        str: The URL of the bot API.
    """
    return get_vault_secret(secret_path)["data"]["url"]


@task
def send_whatsapp_message(bot_api_url: str, chat_id: str, message: str):
    """
    Sends a message through a Whatsapp Bot.

    Args:
        bot_api_url (str): The URL of the bot.
        chat_id (str): The chat ID of the bot.
        message (str): The message to be sent.

    Returns:
        None

    Raises:

    """
    data = {
        "chat_id": chat_id,
        "message": message,
    }
    try:
        response = requests.post(bot_api_url, json=data)
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        if response.status_code == 404:
            raise WhatsAppBotNotFoundError("Whatsapp Bot not found.") from exc
        raise WhatsAppSendMessageError(
            "Error sending message to Whatsapp Bot."
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise WhatsAppSendMessageError(
            "Error sending message to Whatsapp Bot."
        ) from exc
