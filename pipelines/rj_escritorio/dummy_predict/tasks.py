# -*- coding: utf-8 -*-
"""
Tasks for the predict flow example usage.
"""

from random import random

import pandas as pd
import pendulum
from prefect import task

from pipelines.constants import constants


@task(checkpoint=False)
def get_current_timestamp() -> str:
    """
    Get current timestamp.
    """
    return pendulum.now(tz=constants.DEFAULT_TIMEZONE.value).to_datetime_string()


@task(checkpoint=False)
def get_dummy_input_data() -> pd.DataFrame:
    """
    Return some dummy data
    """
    columns = [
        "alcohol",
        "chlorides",
        "citric acid",
        "density",
        "fixed acidity",
        "free sulfur dioxide",
        "pH",
        "residual sugar",
        "sulphates",
        "total sulfur dioxide",
        "volatile acidity",
    ]
    data = [
        [
            random() * 15,
            random(),
            random(),
            random(),
            random() * 7,
            random() * 35,
            random() * 5,
            random() * 2,
            random(),
            random() * 100,
            random(),
        ]
    ]
    return pd.DataFrame(data=data, columns=columns)
