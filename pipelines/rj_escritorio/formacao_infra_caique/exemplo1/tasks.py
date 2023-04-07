# -*- coding: utf-8 -*-
from prefect import task

from pipelines.utils.utils import log


@task
def hello_name(name: str) -> None:
    log(f"Hello {name}!")
