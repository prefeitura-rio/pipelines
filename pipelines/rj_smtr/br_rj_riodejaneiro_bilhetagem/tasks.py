# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_bilhetagem
"""

from datetime import timedelta, datetime
import pandas as pd

from prefect import task

from pipelines.utils.utils import log, get_vault_secret

from pipelines.rj_smtr.constants import constants


@task(checkpoint=False)
def get_datetime_range(
    timestamp: datetime,
    interval_minutes: int = 1,
) -> dict:
    """
    Task to get datetime range

    Args:
        timestamp (datetime): timestamp to get datetime range
        interval_minutes (int): interval in minutes to get datetime range (optional)

    Returns:
        dict: datetime range
    """
    start = (timestamp - timedelta(minutes=interval_minutes)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    end = timestamp.strftime("%Y-%m-%d %H:%M:%S")

    return {"start": start, "end": end}


@task(checkpoint=False, nout=2)
def get_bilhetagem_url(
    datetime_range: dict,
    engine: str = "postgres",
) -> tuple:
    """
    Task to get bilhetagem url

    Args:
        timestamp (datetime): timestamp to get bilhetagem url
        interval_minutes (int): interval in minutes to get bilhetagem url (optional)
        engine (str): engine to get bilhetagem url (optional)

    Returns:
        tuple: bilhetagem url and params
    """

    base_params = get_vault_secret(constants.BILHETAGEM_SECRET_PATH.value)["data"]

    base_params["vpn_url"] = base_params["vpn_url"] + engine

    params = {
        "query": f"""   SELECT COUNT(*)
                        FROM transacao
                        WHERE data_processamento BETWEEN '{datetime_range["start"]}'
                        AND '{datetime_range["end"]}'"""
    }

    return base_params, params


@task(checkpoint=False)
def get_bilhetagem_params(
    count_rows: dict,
    datetime_range: dict,
    limit: int = 1000,
) -> list:
    """
    Task to get bilhetagem params

    Args:
        count_rows (dict): count rows from bilhetagem
        timestamp (datetime): timestamp to get bilhetagem params
        limit (int): limit to get bilhetagem params (optional)
        interval_minutes (int): interval in minutes to get bilhetagem params (optional)

    Returns:
        list: bilhetagem query params
    """

    count_rows = pd.DataFrame(count_rows["data"]).iloc[0, 0]

    if count_rows == 0:
        count_rows = 1

    query_params = []

    for i in range(0, count_rows, limit):
        query_params.append(
            {
                "query": f"""   SELECT *
                                FROM transacao
                                WHERE data_processamento BETWEEN '{datetime_range["start"]}'
                                AND '{datetime_range["end"]}'
                                LIMIT {limit}
                                OFFSET {i}"""
            }
        )

    return query_params
