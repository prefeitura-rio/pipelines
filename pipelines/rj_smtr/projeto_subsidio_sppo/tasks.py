# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from prefect import task


@task
def set_run_vars(run_date):
    return {"run_date": run_date}
