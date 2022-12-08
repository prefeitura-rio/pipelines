# -*- coding: utf-8 -*-
"""
Tasks for materialize_to_datario
"""

from typing import Dict
import datetime
from prefect import task

from pipelines.utils.tasks import log


@task
def get_run_date(input_date: str) -> Dict:
    """
    Generates a dict of a date based on input_date
    """
    if input_date is not False:
        date = {"run_date": input_date}
    else:
        date = {"run_date": datetime.date.today().strftime("%Y-%m-%d")}

    log(f"Will run the following date: {date}")
    return date
