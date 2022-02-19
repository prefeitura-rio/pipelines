"""
Tasks for cor
"""
from time import strftime
from typing import List, Tuple

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.utils import (
    get_vault_secret,
    send_telegram_message,
    smart_split,
)


@task(checkpoint=False, nout=2)
def get_token_and_group_id(secret_path: str) -> Tuple[str, int]:
    """
    Returns Telegram token and group ID from a secret file.
    """
    secret = get_vault_secret(secret_path, client=None)
    return (
        secret["data"]["token"].strip(),
        int(secret["data"]["group_id"].strip()),
    )


@task(checkpoint=False)
def get_data() -> pd.DataFrame:
    """
    Returns the dataframe with the alerts.
    """
    query = """
    SELECT *
    FROM `rj-escritorio-dev.transporte_rodoviario_waze.alertas_tratadas_semaforos`
    """
    return bd.read_sql(query=query, billing_project_id="datario", from_file=True)


@task(checkpoint=False)
def format_message(dataframe: pd.DataFrame) -> List[str]:
    """
    Formats the message before sending it.
    """
    # Create a link for eath alert on google maps
    def map_link(street, latlong):
        latlong = str(latlong).replace(", ", "%2C")
        url = "https://www.google.com/maps/search/?api=1&query=" + latlong + "&zoom=21"
        url = '<a href="' + url + '">' + street + "</a>"
        return url

    # Create the alert message
    thumbs_up_emoji = "\U0001F44D"

    # Builds all alert messages
    alert = ""
    for row in range(len(dataframe)):
        identification = str(dataframe.iloc[row]["name"])
        latlong = str(dataframe.iloc[row]["latlong"])
        address = str(dataframe.iloc[row]["description"])
        thumbs_up = str(dataframe.iloc[row]["sum_thumbs_up"])

        alert += (
            identification
            + " - "
            + str(map_link(address, latlong))
            + " - "
            + thumbs_up
            + thumbs_up_emoji
            + "\n \n"
        )

    # Builds the header of the message
    traffic_light_emoji = "\U0001F6A6"
    msg_header = (
        traffic_light_emoji
        + " CETRIO"
        + "\n \nALERTA WAZE - Semáforo quebrado - atualizado em "
        + strftime("%Y-%m-%d %H:%M:%S")
        + "\n \n"
    )

    # Builds final message
    msg = msg_header + alert

    return smart_split(
        text=msg,
        max_length=constants.TELEGRAM_MAX_MESSAGE_LENGTH.value,
        separator="\n",
    )


@task(checkpoint=False)
# pylint: disable=too-many-arguments
def send_messages(
    token: str, group_id: str, messages: List[str]
) -> None:
    """
    Sends the alerts to the Telegram group.
    """
    for message in messages:
        if message != "":
            send_telegram_message(
                message=message, token=token, chat_id=group_id)

    url = (
        '<a href="https://datastudio.google.com/reporting/b2841cf6-dd1b-4700-b6a4-140495e93ff4">'
        + "MAPA GERAL</a>"
    )
    send_telegram_message(message=url, token=token, chat_id=group_id)
