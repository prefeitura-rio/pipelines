# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_bilhetagem
"""

from prefect import task

from datetime import timedelta, datetime
import pandas as pd
from pytz import timezone

from pipelines.utils.utils import log, get_vault_secret

from pipelines.rj_smtr.constants import constants


@task(checkpoint=False)
def get_datetime_range(
    timestamp: datetime,
    interval_minutes: int = 1,
) -> dict:
    """
    Task to get datetime range in UTC

    Args:
        timestamp (datetime): timestamp to get datetime range
        interval_minutes (int): interval in minutes to get datetime range (optional)

    Returns:
        dict: datetime range
    """
    start = (
        (timestamp - timedelta(minutes=interval_minutes))
        .astimezone(tz=timezone("UTC"))
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    end = timestamp.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    return {"start": start, "end": end}


@task(checkpoint=False, nout=3)
def get_bilhetagem_url(
    datetime_range: dict,
    database: str = "transacao_db",
    table_name: str = "transacao",
    table_column: str = "data_processamento",
    method: str = "between",
) -> tuple:
    """
    Task to get bilhetagem url

    Args:
        datetime_range (dict): datetime range to get bilhetagem url
        database (str): database to get bilhetagem url (optional)
        engine (str): engine to get bilhetagem url (optional)
        table_name (str): table name to get bilhetagem url (optional)
        table_column (str): table column to get bilhetagem url (optional)
        method (str): method to get bilhetagem url (optional)

    Returns:
        tuple: bilhetagem url and params
    """

    secrets = get_vault_secret(constants.BILHETAGEM_SECRET_PATH.value)["data"]

    database_secrets = secrets["databases"][database]

    url = secrets["vpn_url"] + database_secrets["engine"]

    base_params = {
        "host": database_secrets["host"],
        "database": database_secrets["database"],
    }

    if method == "between":
        time_cond = f"""WHERE {table_column} BETWEEN '{datetime_range["start"]}'
                        AND '{datetime_range["end"]}'"""
    else:
        time_cond = f"""WHERE {table_column} {method} '{datetime_range["start"]}'"""

    params = {
        "query": f"""   SELECT COUNT(*)
                        FROM {table_name}
                        {time_cond}"""
    }

    log(f"params: {params}")
    log(f"url: {url}")

    return base_params, params, url


@task(checkpoint=False, nout=2)
def get_bilhetagem_params(
    count_rows: dict,
    datetime_range: dict,
    table_name: str = "transacao",
    table_column: str = "data_processamento",
    method: str = "between",
    limit: int = 1000,
) -> tuple:
    """
    Task to get bilhetagem params

    Args:
        count_rows (dict): count rows from bilhetagem
        timestamp (datetime): timestamp to get bilhetagem params
        table_name (str): table name to get bilhetagem params (optional)
        table_column (str): table column to get bilhetagem params (optional)
        method (str): method to get bilhetagem params (optional)
        limit (int): limit to get bilhetagem params (optional)

    Returns:
        flag_get_data (bool): flag to get bilhetagem params
        list: bilhetagem params
    """

    count_rows = pd.DataFrame(count_rows["data"]).iloc[0, 0]

    if count_rows == 0:
        log("No data to get")
        return False, []

    query_params = []

    if method == "between":
        time_cond = f"""WHERE {table_column} BETWEEN '{datetime_range["start"]}'
                        AND '{datetime_range["end"]}'"""
    else:
        time_cond = f"""WHERE {table_column} {method} '{datetime_range["start"]}'"""

    log(f"time_cond: {time_cond}")
    for offset in range(0, count_rows, limit):
        query_params.append(
            {
                "query": f"""   SELECT *
                                FROM {table_name}
                                {time_cond}
                                ORDER BY {table_column}
                                LIMIT {limit}
                                OFFSET {offset}"""
            }
        )

    log(f"{str(count_rows)} rows to get")
    log(query_params)

    return True, query_params


@task(checkpoint=False)
def generate_bilhetagem_flow_params(
    timestamp: datetime, datetime_range: dict, tables_params: dict
) -> list:
    """
    Task to generate bilhetagem flow params

    Args:
        timestamp (datetime): timestamp to generate bilhetagem flow params
        datetime_range (dict): datetime range to generate bilhetagem flow params
        tables_params (dict): tables params to generate bilhetagem flow params

    Returns:
        list: bilhetagem flow params
    """

    flow_params = []

    log(tables_params)

    for table_id in tables_params:
        flow_params.append(
            {
                "timestamp": timestamp,
                "datetime_range": datetime_range,
                "tables_params": tables_params[table_id] | {"table_id": table_id},
            }
        )

    log(flow_params)

    return flow_params
