# -*- coding: utf-8 -*-
"""
Tasks for the backfill flow
"""

from time import sleep
from typing import Any, Dict

import pendulum
from prefect import Client, task
from prefect.engine.state import State

from pipelines.utils.utils import log, run_registered


@task
def create_timestamp_parameters(
    start: pendulum.DateTime,
    end: pendulum.DateTime,
    interval: pendulum.Duration,
    datetime_format: str = "YYYY-MM-DD",
    reverse: bool = False,
):
    """
    Create a list of parameters for a flow that takes timestamp parameters.

    Args:
        start: The start of the range of timestamps to generate
        end: The end of the range of timestamps to generate
        interval: The interval between timestamps
        datetime_format: The format to use for the timestamps
        reverse: Whether to reverse the order of the timestamps

    Returns:
        A list of parameters for a flow that takes timestamp parameters
    """
    parameters = []
    while start < end:
        this_end = start + interval
        parameters.append(
            {
                "start": start.format(datetime_format),
                "end": this_end.format(datetime_format),
            }
        )
        start = this_end
    if reverse:
        parameters.reverse()
    return parameters


@task
# pylint: disable=too-many-arguments
def launch_flow(
    flow_name: str,
    parameter: Dict[str, Any],
    agent_label: str,
    flow_project: str = "main",
    parameter_defaults: Dict[str, Any] = None,
    help_name: str = None,
    datetime_start_param: str = None,
    datetime_end_param: str = None,
    fetch_flow_run_info_sleep_time: int = 30,
    prefect_client: Client = None,
):
    """
    Launch a flow with the given parameters.

    Args:
        flow_name: The name of the flow to launch
        parameter: The backfill parameters to use when launching the flow
        agent_label: The agent label to use when launching the flow
        flow_project: The project to use when launching the flow
        parameter_defaults: The default parameters to use when launching the flow
        help_name: A help name to use when setting the flow run name
        datetime_start_param: The name of the start datetime parameter
        datetime_end_param: The name of the end datetime parameter
        fetch_flow_run_info_sleep_time: The time to sleep between fetching flow run info
        prefect_client: The Prefect client to use

    Returns:
        None
    """

    if parameter_defaults is None:
        parameter_defaults = {}

    if help_name is None:
        help_name = flow_name

    if prefect_client is None:
        prefect_client = Client()

    if datetime_start_param:
        parameter_defaults[datetime_start_param] = parameter["start"]

    if datetime_end_param:
        parameter_defaults[datetime_end_param] = parameter["end"]

    log(f"Launching run for window {parameter['start']} to {parameter['end']}")

    flow_run_id = run_registered(
        flow_name=flow_name,
        flow_project=flow_project,
        labels=[agent_label],
        parameters=parameter_defaults,
        run_description=f"Backfill - {help_name} - {parameter['start']} to {parameter['end']}",
    )

    state: State = prefect_client.get_flow_run_info(flow_run_id=flow_run_id).state

    while not state.is_finished():
        sleep(fetch_flow_run_info_sleep_time)
        state = prefect_client.get_flow_run_info(flow_run_id=flow_run_id).state

    if state.is_successful():
        log(f"Run for window {parameter['start']} to {parameter['end']} succeeded")
    else:
        log(
            f"Run for window {parameter['start']} to {parameter['end']} failed",
            level="error",
        )
        raise Exception(
            f"Run for window {parameter['start']} to {parameter['end']} failed"
        )


@task
def parse_datetime(
    datetime_string: str,
    timezone: str = "America/Sao_Paulo",
) -> pendulum.DateTime:
    """
    Parse a datetime string.

    Args:
        datetime_string: The datetime string to parse
        timezone: The timezone to use

    Returns:
        The parsed datetime
    """
    return pendulum.parse(datetime_string, strict=False, tz=timezone)


@task
def parse_duration(
    duration_dict: Dict[str, int],
) -> pendulum.Duration:
    """
    Parse a duration dictionary.

    Args:
        duration_dict: The duration dictionary to parse

    Returns:
        The parsed duration
    """
    return pendulum.duration(**duration_dict)
