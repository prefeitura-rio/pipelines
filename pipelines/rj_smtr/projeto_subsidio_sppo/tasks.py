# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from typing import List
import datetime
import pandas as pd
from prefect import task

from pipelines.utils.tasks import log


@task
def get_run_dates(date_range_start: str, date_range_end: str = None) -> List:
    """
    Generates a list of dates between date_range_start and date_range_end.
    """
    if (date_range_end is None) and (date_range_start is not False):
        dates = [{"run_date": date_range_start}]
    elif (date_range_start is False) or (date_range_end is False):
        dates = [{"run_date": datetime.date.today().strftime("%Y-%m-%d")}]
    else:
        dates = [
            {"run_date": d.strftime("%Y-%m-%d")}
            for d in pd.date_range(start=date_range_start, end=date_range_end)
        ]

    log(f"Will run the following dates: {dates}")
    return dates


# @task
# def log_date(msg: str, date_1: str, date_2: str):
#     """
#     Generates a log based on date_1 and date _2
#     """
#     log(f"{msg} (date_1: {date_1} | date_2: {date_2})")

# @task
# def get_run_date(date_1: str, date_2: str) -> str:
#     """
#     Chooses between date_1 and date _2
#     """
#     if(date_1 is None):
#         return date_2
#     else:
#         return date_1
