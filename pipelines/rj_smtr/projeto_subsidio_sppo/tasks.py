# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from datetime import datetime, timedelta, date
from typing import List

from prefect import task

import pandas as pd
from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import get_last_run_timestamp, get_table_min_max_value
from pipelines.utils.utils import log


@task
def query_max_planned_date(
    project_id: str = "rj-smtr", mode="prod", last_run_date=None
):
    """_summary_

    Args:
        project_id (str, optional): _description_. Defaults to "rj-smtr".
        mode (str, optional): _description_. Defaults to "prod".
        last_run_date (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    if mode == "dev":
        project_id += "-dev"
    log(f"Querying planned trips in mode: {mode}")
    planned_mdate = get_table_min_max_value(
        query_project_id=project_id,
        dataset_id=constants.SUBSIDIO_SPPO_DATASET_ID.value,
        table_id=constants.SUBSIDIO_SPPO_PLANNED_TABLE_ID.value,
        field_name="data",
        base_value=last_run_date,
    )
    log(f"Planned trips max date is: {planned_mdate}")
    if not isinstance(planned_mdate, date):
        planned_mdate = datetime.strptime(planned_mdate, "%Y-%m-%d")
    return planned_mdate


@task
def get_max_complete_date(mode: str = "prod"):
    """_summary_

    Args:
        mode (str, optional): _description_. Defaults to "prod".

    Returns:
        _type_: _description_
    """
    last_run = get_last_run_timestamp(
        dataset_id=constants.SUBSIDIO_SPPO_DATASET_ID.value,
        table_id=constants.SUBSIDIO_SPPO_COMPLETED_TABLE_ID.value,
        mode=mode,
    )
    log(f"Got last run as: {last_run}")
    return datetime.fromisoformat(last_run).date()


@task
def get_date_range(max_planned_date, max_complete_date):
    """_summary_

    Args:
        max_planned_date (_type_): _description_
        max_complete_date (_type_): _description_

    Returns:
        _type_: _description_
    """
    now_dt = datetime.now().date()
    date_range_start = max_complete_date + timedelta(days=1)
    date_range_end = min(max_planned_date, now_dt)
    date_range = pd.date_range(start=date_range_start, end=date_range_end).to_list()
    if not date_range:
        log("No dates found to run, skiping...")
    return [{"run_date": dt.date().isoformat()} for dt in date_range]
    # 11/10 - now; 30/09 - complete; 15/10 - planned


@task
def get_run_params(date_range: List, dataset_sha: str):
    """_summary_

    Args:
        date_range (List): _description_
        dataset_sha (str): _description_

    Returns:
        _type_: _description_
    """
    for run_date in date_range:
        run_date.update({"version": dataset_sha})
    return date_range
