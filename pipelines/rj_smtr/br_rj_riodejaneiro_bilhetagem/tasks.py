# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_bilhetagem
"""

from prefect import task
from datetime import timedelta, datetime

from pipelines.utils.utils import log, get_vault_secret

from pipelines.rj_smtr.tasks import get_raw
from pipelines.rj_smtr.constants import constants


@task(checkpoint=False, nout=3)
def get_bilhetagem_params(
    timestamp: datetime,
    interval_minutes: int = 1,
    limit: int = 1000,
) -> dict:
    """
    Task to get bilhetagem params

    Args:
        timestamp (datetime): timestamp to get bilhetagem params
        interval_minutes (int): interval in minutes to get bilhetagem params
        limit (int): limit of rows to get bilhetagem params

    Returns:
        dict: bilhetagem params
    """

    log("Starting get_bilhetagem_params")

    datetime_range_start = (timestamp - timedelta(minutes=interval_minutes)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    datetime_range_end = timestamp.strftime("%Y-%m-%d %H:%M:%S")

    secrets = get_vault_secret(constants.BILHETAGEM_SECRET_PATH.value)["data"]

    url = secrets["vpn_url"] + "postgres"

    base_params = {"host": secrets["host"], "database": secrets["database"]}

    params = {
        "query": f"""   SELECT COUNT(*)
                        FROM transacao
                        WHERE data_processamento BETWEEN '{datetime_range_start}'
                        AND '{datetime_range_end}'"""
    }

    # query para saber quantos resultados tem no intervalo de tempo

    log("Starting get_raw")

    count_rows = get_raw.run(
        url=url,
        headers=constants.BILHETAGEM_SECRET_PATH.value,
        base_params=base_params,
        params=params,
    )

    log(count_rows)

    count_rows = count_rows.iloc[0, 0]

    query_params = dict()

    for i in range(0, count_rows, limit):
        query_params[
            "query"
        ] = f"""SELECT *
                                    FROM transacao
                                    WHERE data_processamento BETWEEN '{datetime_range_start}'
                                    AND '{datetime_range_end}'
                                    LIMIT {limit}
                                    OFFSET {i}"""

        yield params

    log(query_params)

    return base_params, query_params, url
