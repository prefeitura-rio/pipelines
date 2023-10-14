# -*- coding: utf-8 -*-
"""
Tasks for dump_api_prontuario_vitacare
"""

from datetime import date
from prefect import task
from pipelines.utils.utils import log


@task
def build_params():
    params = {"date": str(date.today())}
    log(f"Params built: {params}")
    return params
