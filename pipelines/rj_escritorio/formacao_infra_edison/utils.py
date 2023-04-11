# -*- coding: utf-8 -*-
from pipelines.utils.utils import log


def hello_name(name: str) -> None:
    log(f"Olá {name}!")
