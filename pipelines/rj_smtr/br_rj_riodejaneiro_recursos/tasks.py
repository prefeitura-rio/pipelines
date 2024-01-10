# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log


@task
def log_all(value, function) -> None:
<<<<<<< HEAD
    
    log(f'{function}: {value}')
=======
    log(function, value)
>>>>>>> 02c39ae173a51f9c497bc14ed623650ca886c2a1
