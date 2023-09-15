# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
General purpose functions for br_rj_riodejaneiro_bilhetagem
"""

from pipelines.utils.utils import get_vault_secret


def create_bilhetagem_request_params(  # pylint: disable=too-many-arguments,too-many-locals
    datetime_range: dict,
    table_params: dict,
    secret_path: str,
) -> tuple:
    """
    Creates the request parameters for the bilhetagem tables

    Args:
        datetime_range (dict): The datetime range to be used in the query
        table_params (dict): The table parameters
        secret_path (str): The secret path to get the database credentials

    Returns:
        tuple: The request parameters and the url
    """

    secrets = get_vault_secret(secret_path)["data"]

    database_secrets = secrets["databases"][table_params["database"]]

    url = secrets["vpn_url"] + database_secrets["engine"]

    if table_params["method"] == "between":
        time_cond = f"""WHERE
                            {table_params["table_column"]} BETWEEN '{datetime_range["start"]}'
                            AND '{datetime_range["end"]}'"""
    else:
        time_cond = f"""WHERE
                            {table_params["table_column"]} {table_params["method"]} '{datetime_range["start"]}'"""

    params = {
        "host": database_secrets["host"],
        "database": table_params["database"],
        "query": f"""   SELECT
                            *
                        FROM
                            {table_params["table_name"]}
                        {time_cond}
                        ORDER BY
                            {table_params["table_column"]}""",
    }

    return params, url
