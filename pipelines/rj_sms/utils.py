# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
General utilities for sms pipelines.
"""

from datetime import datetime, timedelta
from typing import List

from prefect.schedules.clocks import IntervalClock


def generate_dump_api_schedules(  # pylint: disable=too-many-arguments,too-many-locals
    interval: timedelta,
    start_date: datetime,
    labels: List[str],
    flow_run_parameters: List[dict],
    runs_interval_minutes: int = 2,
) -> List[IntervalClock]:
    """
    Generates multiple schedules for vitacare dumping.
    """
    clocks = []
    for count, parameters in enumerate(flow_run_parameters):
        new_interval = parameters["interval"] if "interval" in parameters else interval

        clocks.append(
            IntervalClock(
                interval=new_interval,
                start_date=start_date
                + timedelta(minutes=runs_interval_minutes * count),
                labels=labels,
                parameter_defaults=parameters,
            )
        )
    return clocks


def generate_dicts(dict_template: dict, key: str, values: list) -> list:
    """
    Generates a list of dictionaries from a template dictionary and a list of
    values to be used in the template.

    Args:
        dict_template (dict): Template dictionary to be used in the generation
            of the list of dictionaries.
        key (str): Key to be used in the template.
        values (list): List of values to be used in the template.

    Returns:
        list: List of dictionaries generated from the template dictionary and
            the list of values.
    """
    return [dict_template | {key: value} for value in values]
