# -*- coding: utf-8 -*-
from prefect import Parameter

from pipelines.rj_escritorio.cleanup.tasks import (
    delete_flow_run,
    get_old_flow_runs,
    get_prefect_client,
)
from pipelines.utils.decorators import Flow

with Flow(
    "EMD: Limpeza de histórico de runs",
    code_owners=[
        "gabriel",
    ],
) as database_cleanup_flow:

    # Parameters
    days_old = Parameter("days_old", default=60, required=False)

    # Get the Prefect client
    client = get_prefect_client()

    # Get the old flow runs
    old_flow_runs = get_old_flow_runs(days_old=days_old, client=client)

    # Delete the old flow runs
    delete_flow_run.map(old_flow_runs)
