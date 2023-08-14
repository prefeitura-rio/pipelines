# -*- coding: utf-8 -*-
from datetime import timedelta
import traceback
import pandas as pd
from prefect import task


@task
def test_value_error():
    raise ValueError("This is a Error for test purposes only")
