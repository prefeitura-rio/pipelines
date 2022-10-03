# -*- coding: utf-8 -*-
"""
Tasks for INEA.
"""
from os import environ

from prefect import task

from pipelines.utils.utils import log


@task
def print_environment_variables():
    """
    Print all environment variables
    """
    log("Environment variables:")
    for key, value in environ.items():
        log(f"{key}={value}")
