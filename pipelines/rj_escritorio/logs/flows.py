# -*- coding: utf-8 -*-
"""
Flow definition for daily logs materialization.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_escritorio.logs.schedules import daily_at_4am
from pipelines.utils.execute_dbt_model.flows import utils_run_dbt_model_flow
from pipelines.utils.utils import set_default_parameters

materialize_logs_flow = deepcopy(utils_run_dbt_model_flow)
materialize_logs_flow.name = "EMD: logs - Materialize logs"
materialize_logs_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
materialize_logs_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
)

materialize_logs_flow_default_parameters = {
    "dataset_id": "logs_bq",
    "table_id": "queries_all_projects_materialized",
    "mode": "prod",
}
materialize_logs_flow = set_default_parameters(
    materialize_logs_flow, default_parameters=materialize_logs_flow_default_parameters
)

materialize_logs_flow.schedule = daily_at_4am
