"""
Tasks for the daily birthday flow.
"""

from typing import List

import pendulum
from prefect import task
import pytz
import requests

from pipelines.utils import (
    get_vault_secret,
    send_discord_message,
)


@task
def get_todays_date() -> str:
    """
    Get today's date in format dd-mm
    """
    return pendulum.now().replace(
        tzinfo=pytz.timezone("America/Sao_Paulo")).strftime("%d-%m")


@task
def get_birthdays_by_date(date: str) -> List[str]:
    """
    Get birthdays by date.
    """
    birthdays_url = f"http://bot-rio-api.bot-rio.svc.cluster.local/users/?birthday={date}"
    response = requests.get(birthdays_url)
    response.raise_for_status()
    return response.json()["results"]


@task
def send_birthday_message(names: List[str], secret_path: str) -> None:
    """
    Send birthday message.
    """
    secret = get_vault_secret(secret_path)
    webhook_url = secret["data"]["url"]
    for name in names:
        message = f"Tem aniversário hoje!!! Parabéns {name}! 🥳🥳🥳"
        send_discord_message(message, webhook_url)
