# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from prefect import task


@task
def check_param(param: str) -> bool:
    """
    Check if param is None
    """
    return param is None
