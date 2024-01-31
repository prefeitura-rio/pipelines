# -*- coding: utf-8 -*-
"Flows for janitor"
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow

# from pipelines.rj_escritorio.cleanup.tasks import cancel_flow_run

from pipelines.rj_smtr.schedules import every_10_minutes
from pipelines.rj_smtr.janitor.tasks import (
    get_active_flow_names,
    query_archived_scheduled_runs,
    cancel_flow_runs,
)

with Flow(
    "SMTR: Desagendamento de runs arquivadas", code_owners=["caio"]
) as janitor_flow:
    flow_names = get_active_flow_names()
    archived_flow_runs = query_archived_scheduled_runs.map(flow_name=flow_names)
    cancel_flow_runs.map(flow_runs=archived_flow_runs)

janitor_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
janitor_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
janitor_flow.schedule = every_10_minutes
