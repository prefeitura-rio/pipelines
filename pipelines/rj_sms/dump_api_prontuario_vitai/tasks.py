# -*- coding: utf-8 -*-
"""
Tasks for dump_api_prontuario_vitai
"""

from datetime import date, timedelta
from prefect import task
from pipelines.utils.utils import log


@task
def build_movimentos_url(date_param=None):
    if date_param is None:
        date_param = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")

    url = f"https://apidw.vitai.care/api/dw/v1/movimentacaoProduto/query/dataMovimentacao/{date_param}"  # noqa: E501
    log(f"URL built: {url}")
    return url


@task
def build_movimentos_date(date_param=None):
    if date_param is None:
        date_param = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")

    return date_param
