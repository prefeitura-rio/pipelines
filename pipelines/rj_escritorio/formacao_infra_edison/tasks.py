# -*- coding: utf-8 -*-
from prefect import task
import requests

from pipelines.utils.utils import log


@task
def hello_name(name: str) -> None:
    log(f"Olá {name}!")
