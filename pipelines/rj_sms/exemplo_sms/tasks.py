# -*- coding: utf-8 -*-
from prefect import task

from pipelines.utils.utils import log


@task
def say_hello(name: str) -> None:
    log(f"Hello {name}!")
