# -*- coding: utf-8 -*-
# pylint: disable=W0102
"""
General utilities for backfill pipelines.
"""

from typing import Any, Dict

import pendulum
import prefect

from pipelines.utils.utils import log


def run_local_backfill(
    flow: prefect.Flow,
    parameters: Dict[str, Any] = {},
    backfill_parameters: Dict[str, Any] = None,
):
    """
    Runs backfill flow locally.
    It runs backwards in time, so it runs the most recent dates first.

    Mandatory backfill_parameters:
    * start_date: Start date for backfill
    * end_date: End date for backfill
    * format_date: Format in which dates were passed
    * interval: Number of time interval
    * interval_period: If the skip is in hours, days or weeks
    Note: keepthe same formar for start_date and end_date

    Example:
    backfill_parameters = {
        'start_date': '2022-03-01 00:20:00',
        'end_date': '2022-03-01 04:20:00',
        'format_date': 'YYYY-MM-DD HH:mm:ss',
        'interval': '1',
        'interval_period': 'hours'
    }
    run_local_backfill(flow, backfill_parameters=backfill_parameters)
    """
    # Setup for local run
    flow.storage = None
    flow.run_config = None
    flow.schedule = None

    if sorted(backfill_parameters.keys()) != [
        "end_date",
        "format_date",
        "interval",
        "interval_period",
        "mode_redis",
        "start_date",
    ]:
        log(
            "Your parameter input has missing information. Check if you have all of this\
             parameters: start_date, end_date, format_date, interval, interval_period"
        )
        return

    start_date = backfill_parameters["start_date"]
    end_date = backfill_parameters["end_date"]
    format_date = backfill_parameters["format_date"]
    interval = int(backfill_parameters["interval"])
    interval_period = backfill_parameters["interval_period"]
    mode_redis = backfill_parameters["mode_redis"]

    if interval_period not in ("minutes", "hours", "days", "weeks"):
        log(
            "interval_period only accepts minutes, hours, days, weeks. Change the code if necessary"
        )
        return

    start_date = pendulum.from_format(start_date, format_date)
    end_date = pendulum.from_format(end_date, format_date)

    while start_date < end_date:
        # Run flow
        parameters["current_time"] = end_date.to_datetime_string()
        parameters["mode_redis"] = mode_redis
        flow.run(parameters=parameters)
        # Update end_date backwards
        if interval_period == "minutes":
            end_date = end_date.subtract(minutes=interval)
        if interval_period == "hours":
            end_date = end_date.subtract(hours=interval)
        elif interval_period == "days":
            end_date = end_date.subtract(days=interval)
        if interval_period == "weeks":
            end_date = end_date.subtract(weeks=interval)


def run_local_missing(
    flow: prefect.Flow,
    parameters: Dict[str, Any] = {},
    backfill_parameters: Dict[str, Any] = None,
):
    """
    Runs backfill flow locally.
    It runs backwards in time, so it runs the most recent dates first.

    Mandatory backfill_parameters:
    * start_date: Start date for backfill
    * end_date: End date for backfill
    * format_date: Format in which dates were passed
    * interval: Number of time interval
    * interval_period: If the skip is in hours, days or weeks
    Note: keepthe same formar for start_date and end_date

    Example:
    backfill_parameters = {
        'start_date': '2022-03-01 00:20:00',
        'end_date': '2022-03-01 04:20:00',
        'format_date': 'YYYY-MM-DD HH:mm:ss',
        'interval': '1',
        'interval_period': 'hours'
    }
    run_local_backfill(flow, backfill_parameters=backfill_parameters)
    """
    # Setup for local run
    flow.storage = None
    flow.run_config = None
    flow.schedule = None

    # if sorted(backfill_parameters.keys()) != ['end_date', 'format_date',
    #                                           'interval', 'interval_period', 'start_date']:
    #     log("Your parameter input has missing information. Check if you have all of this\
    #          parameters: start_date, end_date, format_date, interval, interval_period")
    #     return

    date_list = backfill_parameters["date_list"]
    # save_removed_dates = []

    while len(date_list):
        # Run flow
        parameters["current_time"] = date_list[0]
        flow.run(parameters=parameters)
        # Update end_date backwards
        date_list.pop(0)
        #  removed = date_list.pop(0)
    #     save_removed_dates.append(removed)

    # print(f"Items removed: {save_removed_dates}")
