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

    params = {
        "host": database_secrets["host"],
        "database": table_params["database"],
        "query": table_params["query"].format(**datetime_range)
    }

    return params, url
