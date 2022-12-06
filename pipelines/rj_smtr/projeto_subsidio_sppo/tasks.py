# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from typing import List
import datetime
import pandas as pd
from pipelines.utils.tasks import log

from prefect import task


@task
def get_run_dates(date_range_start: str, date_range_end: str) -> List:
    """
    Generates a list of dates between date_range_start and date_range_end.
    """
    if (date_range_start is False) or (date_range_end is False):
        return [{"run_date": datetime.date.today().strftime("%Y-%m-%d")}]

    return [
        {"run_date": d.strftime("%Y-%m-%d")}
        for d in pd.date_range(start=date_range_start, end=date_range_end)
    ]


@task
def get_run_date(run_date: str, interval_days: int = 1) -> str:
    """
    Get previous date base on run_date and interval_days
    """
    if run_date is None:
        previous_date = datetime.date.today() - datetime.timedelta(days=interval_days)
        log(
            f"run_date unprovided. Using today {datetime.date.today().strftime('%Y-%m-%d')}"
        )
    else:
        run_date = datetime.datetime.strptime(run_date, "%Y-%m-%d")
        previous_date = run_date - datetime.timedelta(days=interval_days)
        log(f"run_date {run_date.strftime('%Y-%m-%d')} provided")

    log(
        f"previous date {previous_date.strftime('%Y-%m-%d')} based on interval_days {interval_days}"
    )

    return {"run_date": previous_date.strftime("%Y-%m-%d")}
