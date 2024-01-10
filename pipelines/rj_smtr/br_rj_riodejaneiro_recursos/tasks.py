# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log


@task
def log_all(value, function) -> None:
    log(f'{function}: {value}')
