# -*- coding: utf-8 -*-
"""
Useful functions for br_rj_riodejaneiro_bilhetagem
"""

from datetime import timedelta, datetime
from typing import List

from prefect.schedules.clocks import IntervalClock

from pipelines.utils.utils import log


def generate_execute_bilhetagem_schedules(  # pylint: disable=too-many-arguments,too-many-locals
    interval: timedelta,
    start_date: datetime,
    labels: List[str],
    table_parameters: dict,
    runs_interval_minutes: int = 15,
) -> List[IntervalClock]:
    """
    Generates multiple schedules for executing the bilhetagem pipeline
    """
    clocks = []
    for count, (table_id, parameters) in enumerate(table_parameters.items()):
        parameter_defaults = {
            "tables_params": parameters | {"table_id": table_id},
        }
        log(f"parameter_defaults: {parameter_defaults}")
        clocks.append(
            IntervalClock(
                interval=interval,
                start_date=start_date
                + timedelta(minutes=runs_interval_minutes * count),
                labels=labels,
                parameter_defaults=parameter_defaults,
            )
        )
    return clocks
