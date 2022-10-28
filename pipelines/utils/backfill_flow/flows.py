# -*- coding: utf-8 -*-
"""
Backfill flow definition
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

from pipelines.constants import constants
from pipelines.utils.backfill_flow.tasks import (
    create_timestamp_parameters,
    launch_flow,
    parse_datetime,
    parse_duration,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow


with Flow(
    name=utils_constants.FLOW_BACKFILL_NAME.value,
    code_owners=[
        "gabriel",
    ],
) as backfill_flow:

    # Parameters
    agent_label = Parameter("agent_label")
    backfill_end = Parameter("backfill_end")
    backfill_interval = Parameter("backfill_interval", default={"days": 1})
    backfill_start = Parameter("backfill_start")
    datetime_end_param = Parameter("datetime_end_param", default=None, required=False)
    datetime_format = Parameter("datetime_format", default="YYYYMMDD")
    datetime_start_param = Parameter(
        "datetime_start_param", default=None, required=False
    )
    fetch_flow_run_info_sleep_time = Parameter(
        "fetch_flow_run_info_sleep_time",
        default=30,
    )
    flow_name = Parameter("flow_name")
    flow_project = Parameter("flow_project", default="main")
    help_name = Parameter("help_name", default=None, required=False)
    parameter_defaults = Parameter("parameter_defaults", default=None, required=False)
    reverse = Parameter("reverse", default=True)

    # Parse inputs
    backfill_start_datetime = parse_datetime(
        datetime_string=backfill_start,
    )
    backfill_end_datetime = parse_datetime(
        datetime_string=backfill_end,
    )
    backfill_interval_duration = parse_duration(
        duration_dict=backfill_interval,
    )

    # Create timestamp parameters
    timestamp_parameters = create_timestamp_parameters(
        start=backfill_start_datetime,
        end=backfill_end_datetime,
        interval=backfill_interval_duration,
        format=datetime_format,
        reverse=reverse,
    )

    # Launch runs
    launch_flow.map(
        flow_name=unmapped(flow_name),
        parameter=timestamp_parameters,
        agent_label=unmapped(agent_label),
        flow_project=unmapped(flow_project),
        parameter_defaults=unmapped(parameter_defaults),
        help_name=unmapped(help_name),
        datetime_start_param=unmapped(datetime_start_param),
        datetime_end_param=unmapped(datetime_end_param),
        fetch_flow_run_info_sleep_time=unmapped(fetch_flow_run_info_sleep_time),
    )

backfill_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
backfill_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
