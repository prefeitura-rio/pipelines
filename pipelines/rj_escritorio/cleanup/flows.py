# -*- coding: utf-8 -*-
"""
Flow definitions for the cleanup pipeline.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

from pipelines.rj_escritorio.cleanup.schedules import (
    daily_at_3am_cleanup,
    daily_at_3am_running_flows,
)
from pipelines.rj_escritorio.cleanup.tasks import (
    cancel_flow_run,
    delete_flow_run,
    get_old_flow_runs,
    get_old_running_flow_runs,
    get_prefect_client,
)
from pipelines.constants import constants
from pipelines.utils.decorators import Flow

with Flow(
    "EMD: Limpeza de histórico de runs",
    code_owners=[
        "gabriel",
    ],
) as database_cleanup_flow:

    # Parameters
    days_old = Parameter("days_old", default=60, required=False)
    skip_running = Parameter("skip_running", default=True, required=False)

    # Get the Prefect client
    client = get_prefect_client()

    # Get the old flow runs
    old_flow_runs = get_old_flow_runs(
        days_old=days_old, client=client, skip_running=skip_running
    )

    # Delete the old flow runs
    delete_flow_run.map(old_flow_runs)

database_cleanup_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
database_cleanup_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
database_cleanup_flow.schedule = daily_at_3am_cleanup

with Flow(
    "EMD: Limpeza de runs em execução",
    code_owners=[
        "gabriel",
    ],
) as rj_escritorio__cleanup__running_flows_cleanup:

    # Parameters
    older_than_days = Parameter("older_than_days", default=14, required=False)

    # Get the Prefect client
    client = get_prefect_client()

    # Get the flow runs
    flow_runs = get_old_running_flow_runs(
        older_than_days=older_than_days, client=client
    )

    # Delete the old flow runs
    cancel_flow_run.map(flow_run_dict=flow_runs, client=unmapped(client))

rj_escritorio__cleanup__running_flows_cleanup.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
rj_escritorio__cleanup__running_flows_cleanup.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
rj_escritorio__cleanup__running_flows_cleanup.schedule = daily_at_3am_running_flows
