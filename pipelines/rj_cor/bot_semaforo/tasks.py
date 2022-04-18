# -*- coding: utf-8 -*-
"""
Tasks for cor
"""
from typing import List, Tuple
from datetime import datetime, timedelta

import basedosdados as bd
import pandas as pd
from prefect import task
import pytz

from pipelines.constants import constants
from pipelines.utils.utils import (
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


# @task(checkpoint=False)
def get_data() -> pd.DataFrame:
    """
    Returns the dataframe with the alerts.
    """
    query = """
    WITH semaforos AS (
    SELECT
    *,
    ST_BUFFER(geometry, 100) raio
    FROM `rj-escritorio-dev.transporte_rodoviario_cet.semaforos`),

    distinct_selection AS (
    SELECT
        DISTINCT ts,
        uuid
    FROM
        `rj-escritorio-dev.transporte_rodoviario_waze.alertas`
    WHERE
        type = 'HAZARD'
        AND subtype='HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT'
        AND city = 'Rio de Janeiro'
        AND DATE_DIFF(CURRENT_DATE(), CAST(ts as DATETIME), DAY) < 1 ),
    intermediate_query AS (
        SELECT
        distinct_selection.*,
        original.street,
        original.geometry,
        original.number_thumbs_up,
        original.reliability
        FROM
        distinct_selection
        LEFT JOIN
        `rj-escritorio-dev.transporte_rodoviario_waze.alertas` AS original
        ON
        distinct_selection.ts = original.ts
        AND distinct_selection.uuid = original.uuid
        WHERE
        original.street IS NOT NULL
        AND original.city = "Rio de Janeiro"
        ORDER BY
        ts),
    clusters AS (
        SELECT
            *,
            -- epsilon: The epsilon that specifies the radius, measured in meters,
            -- around a core value. 10 20 50
            ST_CLUSTERDBSCAN(geometry, 50, 1) OVER () AS cluster_num,
            SAFE_CAST(FORMAT_DATE('%s', ts) AS INT64) AS ts_epoch,
        FROM intermediate_query),

    pontos_de_alertas AS (
    SELECT
        cluster_num,
        MAX(ts) as ts,
        MAX(uuid) as uuid,
        MAX(street) as street,
        SUM(
            CASE WHEN number_thumbs_up is not null then CAST(number_thumbs_up as FLOAT64) ELSE 0 END
        ) as number_thumbs_up,
        MAX(reliability) as reliability,
        MAX(ts_epoch) as ts_epoch,
        ST_CENTROID_AGG(geometry) centroid,
        COUNT(*) as number_cluster_alerts
    FROM clusters
    GROUP BY cluster_num
    ORDER BY cluster_num),

    alertas_no_raio AS (
    SELECT
    s.name,
    DATETIME(MIN(ts), 'America/Sao_Paulo') initial_ts,
    s.description,
    SUM(a.number_thumbs_up) sum_thumbs_up
    FROM semaforos as s
    INNER JOIN pontos_de_alertas as a
        ON ST_COVERS(s.raio, a.centroid)
    GROUP BY s.name, s.description)

    SELECT
    alertas_no_raio.*,
    ST_Y(geometry) semaforo_latitude,
    ST_X(geometry) semaforo_longitude
    FROM alertas_no_raio
    LEFT JOIN semaforos
        ON alertas_no_raio.name = semaforos.name
    ORDER BY initial_ts DESC
    """
    return bd.read_sql(query=query, billing_project_id="rj-cor", from_file=True)


# @task(checkpoint=False)
def format_message(dataframe: pd.DataFrame) -> pd.Series:
    """
    Formats the message before sending it.
    """
    # Create a link for eath alert on google maps
    def map_link(dataframe: pd.DataFrame):
        url = (
            "https://www.google.com/maps/search/?api=1&query="
            + dataframe["semaforo_latitude"].astype(str)
            + ","
            + dataframe["semaforo_longitude"].astype(str)
            + "&zoom=21"
        )
        url = '<a href="' + url + '">' + dataframe["description"] + "</a>"
        return url

    def current_date_time():
        """
        Gets current "date and time" and "current date and time minus 1 hour in
        list [current, current_minus_1h]
        """
        date_format = "%Y-%m-%d %H:%M:%S"
        current = datetime.strptime(
            datetime.now(pytz.timezone("America/Sao_Paulo")).strftime(date_format),
            date_format,
        )
        current_minus_1h = current - timedelta(minutes=60)
        return current_minus_1h, current

    # Builds all alert messages
    alert = None
    thumbs_up_emoji = "\U0001F44D"
    current_minus_1h, current = current_date_time()

    mask = (dataframe["initial_ts"] > current_minus_1h) & (
        dataframe["initial_ts"] <= current
    )
    filered_alerts = dataframe[mask]
    if len(filered_alerts) > 0:
        filered_alerts["url"] = map_link(filered_alerts)
        filered_alerts["alert"] = (
            filered_alerts["initial_ts"].apply(lambda x: str(x)[11:16])
            + " - "
            + filered_alerts["name"]
            + " - "
            + filered_alerts["url"]
            + " - "
            + filered_alerts["sum_thumbs_up"].astype(str)
            + thumbs_up_emoji
            + "\n \n"
        )
        alert = "".join(filered_alerts["alert"].tolist())

    traffic_light_emoji = "\U0001F6A6"
    msg_header = (
        traffic_light_emoji
        + " CETRIO"
        + "\n \nALERTA WAZE - Semáforo quebrado - atualizado em "
        + current.strftime("%Y-%m-%d %H:%M:%S")
        + "\n"
        + "Alertas no período de: "
        + current_minus_1h.strftime("%H:%M")
        + " -> "
        + current.strftime("%H:%M")
        + "\n \n"
    )

    # Builds final message
    if alert:
        msg = msg_header + alert
    else:
        alert = "Não foram encontrados alertas no período" + "\n \n"
        msg = msg_header

    return smart_split(
        text=msg,
        max_length=constants.TELEGRAM_MAX_MESSAGE_LENGTH.value,
        separator="\n",
    )


@task(checkpoint=False)
# pylint: disable=too-many-arguments
def send_messages(token: str, group_id: str, messages: List[str]) -> None:
    """
    Sends the alerts to the Telegram group.
    """
    for message in messages:
        if message != "":
            send_telegram_message(message=message, token=token, chat_id=group_id)

    url = (
        '<a href="https://datastudio.google.com/reporting/b2841cf6-dd1b-4700-b6a4-140495e93ff4">'
        + "MAPA GERAL</a>"
    )
    send_telegram_message(message=url, token=token, chat_id=group_id)
