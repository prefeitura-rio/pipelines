# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_bilhetagem
"""
from datetime import timedelta, datetime

from prefect import task

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


@task(checkpoint=False, nout=2)
def get_bilhetagem_request_params(
    datetime_range: dict,
    database: str = "transacao_db",
    table_name: str = "transacao",
    table_column: str = "data_processamento",
    method: str = "between",
) -> tuple:
    """
    Task to get bilhetagem request params

    Args:
        datetime_range (dict): datetime range to get bilhetagem params
        database (str): database to get bilhetagem params (optional)
        table_name (str): table name to get bilhetagem params (optional)
        table_column (str): table column to get bilhetagem params (optional)
        method (str): method to get bilhetagem params (optional)

    Returns:
        params: host, database and query to request data
        url: url to request data
    """

    secrets = get_vault_secret(constants.BILHETAGEM_SECRET_PATH.value)["data"]

    database_secrets = secrets["databases"][database]

    url = secrets["vpn_url"] + database_secrets["engine"]

    if method == "between":
        time_cond = f"""WHERE
                            {table_column} BETWEEN '{datetime_range["start"]}'
                            AND '{datetime_range["end"]}'"""
    else:
        time_cond = f"""WHERE
                            {table_column} {method} '{datetime_range["start"]}'"""

    params = {
        "host": database_secrets["host"],
        "database": database,
        "query": f"""   SELECT
                            *
                        FROM
                            {table_name}
                        {time_cond}
                        ORDER BY
                            {table_column}""",
    }

    log(f"params: {params}")
    log(f"url: {url}")

    return params, url


@task(checkpoint=False)
def generate_bilhetagem_flow_params(
    timestamp: datetime, interval_minutes: int, tables_params: dict
) -> list:
    """
    Task to generate bilhetagem flow params

    Args:
        timestamp (datetime): timestamp to generate bilhetagem flow params
        interval_minutes (int): interval in minutes to generate bilhetagem flow params
        tables_params (dict): tables params to generate bilhetagem flow params

    Returns:
        list: bilhetagem flow params
    """

    flow_params = []

    log(tables_params)

    for table_id in tables_params:
        flow_params.append(
            {
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "interval_minutes": interval_minutes,
                "tables_params": tables_params[table_id] | {"table_id": table_id},
            }
        )

    log(flow_params)

    return flow_params
