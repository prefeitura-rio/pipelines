# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import traceback
import pandas as pd
from prefect import task


@task
def test_value_error():
    if datetime.now().minute % 5 == 0:
        raise ValueError(
            "This is a Error for test purposes only (happens every 5 minutes)"
        )
    return None


@task
def test_type_eror(wait=None):
    raise TypeError("This is a test for TypeErrors")
