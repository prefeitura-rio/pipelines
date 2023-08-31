# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_bilhetagem
"""

from datetime import timedelta, datetime
import pandas as pd

from prefect import task

from pipelines.utils.utils import log, get_vault_secret

from pipelines.rj_smtr.constants import constants


@task(checkpoint=False, nout=2)
def get_bilhetagem_url(
    timestamp: datetime,
    interval_minutes: int = 1,
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

    datetime_range_start = (timestamp - timedelta(minutes=interval_minutes)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    datetime_range_end = timestamp.strftime("%Y-%m-%d %H:%M:%S")

    params = {
        "query": f"""   SELECT COUNT(*)
                        FROM transacao
                        WHERE data_processamento BETWEEN '{datetime_range_start}'
                        AND '{datetime_range_end}'"""
    }

    return base_params, params


@task(checkpoint=False)
def get_bilhetagem_params(
    count_rows: dict,
    timestamp: datetime,
    limit: int = 1000,
    interval_minutes: int = 1,
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

    datetime_range_start = (timestamp - timedelta(minutes=interval_minutes)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    datetime_range_end = timestamp.strftime("%Y-%m-%d %H:%M:%S")

    count_rows = pd.DataFrame(count_rows["data"]).iloc[0, 0]

    if count_rows == 0:
        count_rows = 1

    query_params = []

    for i in range(0, count_rows, limit):
        query_params.append(
            {
                "query": f"""SELECT *
                                FROM transacao
                                WHERE data_processamento BETWEEN '{datetime_range_start}'
                                AND '{datetime_range_end}'
                                LIMIT {limit}
                                OFFSET {i}"""
            }
        )

    return query_params
