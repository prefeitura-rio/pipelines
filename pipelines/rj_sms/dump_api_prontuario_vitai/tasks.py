# -*- coding: utf-8 -*-
"""
Tasks for dump_api_prontuario_vitai
"""

from datetime import timedelta, date, datetime

from prefect import task

from pipelines.utils.utils import log
from pipelines.rj_sms.dump_api_prontuario_vitai.constants import (
    constants as vitai_constants,
)


@task
def build_date_param(date_param: str = "today"):
    """
    Builds a date parameter based on the given input.

    Args:
        date_param (str, optional): The date parameter. Defaults to "today".

    Returns:
        str: The built date parameter.

    Raises:
        ValueError: If the date_param is not a valid date string (YYYY-MM-DD).
    """
    if date_param == "today":
        date_param = str(date.today())
    elif date_param == "yesterday":
        date_param = str(date.today() - timedelta(days=1))
    elif date_param is not None:
        try:
            # check if date_param is a date string
            datetime.strptime(date_param, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError("date_param must be a date string (YYYY-MM-DD)") from e

    log(f"Params built: {date_param}")
    return date_param


@task
def build_url(endpoint: str, date_param: None) -> str:
    """
    Build the URL for the given endpoint and date parameter.

    Args:
        endpoint (str): The endpoint for the URL.
        date_param (None): The date parameter for the URL.

    Returns:
        str: The built URL.
    """
    if date_param is None:
        url = vitai_constants.ENDPOINT.value[endpoint]
    else:
        url = f"{vitai_constants.ENDPOINT.value[endpoint]}/{date_param}"

    log(f"URL built: {url}")

    return url
