"""
Tasks for cor
"""
import math
from time import strftime
from typing import List, Tuple

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.utils import (
    get_vault_secret,
    send_telegram_message,
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


@task(checkpoint=False, nout=4)
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

    # Splits the dataframe into four clusters due to Telegram's 4096 characters limitation
    cluster = math.ceil(len(dataframe) / 4)
    alert1, alert2, alert3, alert4 = "", "", "", ""

    for row in range(len(dataframe)):
        identification = str(dataframe.iloc[row]["name"])
        latlong = str(dataframe.iloc[row]["latlong"])
        address = str(dataframe.iloc[row]["description"])
        thumbs_up = str(dataframe.iloc[row]["sum_thumbs_up"])

        # Splitting the alert message across clusters
        if row < cluster:
            alert1 = (
                alert1
                + identification
                + " - "
                + str(map_link(address, latlong))
                + " - "
                + thumbs_up
                + thumbs_up_emoji
                + "\n \n"
            )
        elif cluster <= row < cluster * 2:
            alert2 = (
                alert2
                + identification
                + " - "
                + str(map_link(address, latlong))
                + " - "
                + thumbs_up
                + thumbs_up_emoji
                + "\n \n"
            )
        elif cluster * 2 <= row < cluster * 3:
            alert3 = (
                alert3
                + identification
                + " - "
                + str(map_link(address, latlong))
                + " - "
                + thumbs_up
                + thumbs_up_emoji
                + "\n \n"
            )
        elif row >= cluster * 3:
            alert4 = (
                alert4
                + identification
                + " - "
                + str(map_link(address, latlong))
                + " - "
                + thumbs_up
                + thumbs_up_emoji
                + "\n \n"
            )

    traffic_light_emoji = "\U0001F6A6"
    msg_alert = (
        traffic_light_emoji
        + " CETRIO"
        + "\n \nALERTA WAZE - Semáforo quebrado - atualizado em "
        + strftime("%Y-%m-%d %H:%M:%S")
        + "\n \n"
    )

    return msg_alert + alert1, alert2, alert3, alert4


@task(checkpoint=False)
# pylint: disable=too-many-arguments
def send_messages(
    token: str, group_id: str, alert1: str, alert2: str, alert3: str, alert4: str
) -> None:
    """
    Sends the alerts to the Telegram group.
    """
    for alert in [alert1, alert2, alert3, alert4]:
        if alert != "":
            send_telegram_message(message=alert, token=token, chat_id=group_id)
    url = (
        '<a href="https://datastudio.google.com/reporting/b2841cf6-dd1b-4700-b6a4-140495e93ff4">'
        + "MAPA GERAL</a>"
    )
    send_telegram_message(message=url, token=token, chat_id=group_id)
