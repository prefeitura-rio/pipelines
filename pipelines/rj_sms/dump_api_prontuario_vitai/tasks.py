# -*- coding: utf-8 -*-
"""
Tasks for dump_api_prontuario_vitai
"""

from datetime import date, timedelta
from prefect import task
from pipelines.utils.utils import log


@task
def build_movimentos_url(date_param=None):
    """
    Builds a URL for querying product movements from the Vitai API.

    Args:
        date_param (str, optional): The date to query in the format "YYYY-MM-DD".
        Defaults to yesterday's date.

    Returns:
        str: The URL for querying product movements from the Vitai API.
    """
    if date_param is None:
        date_param = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")

    url = f"https://apidw.vitai.care/api/dw/v1/movimentacaoProduto/query/dataMovimentacao/{date_param}"  # noqa: E501
    log(f"URL built: {url}")
    return url


@task
def build_movimentos_date(date_param=None):
    """
    Builds a date string in the format '%Y-%m-%d' based on the given date_param or yesterday's
    date if date_param is None.

    Args:
        date_param (str, optional): A date string in the format '%Y-%m-%d'. Defaults to None.

    Returns:
        str: A date string in the format '%Y-%m-%d'.
    """
    if date_param is None:
        date_param = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")

    return date_param
