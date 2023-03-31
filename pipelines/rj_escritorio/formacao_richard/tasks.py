# -*- coding: utf-8 -*-
from perfect import task
from pipelines.utils.utils import log


@task
def add_numbers(number1: int, number2: int) -> None:
    log(f"{number1} + {number2} = {number1 + number2}")
