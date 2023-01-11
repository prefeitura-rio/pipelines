# -*- coding: utf-8 -*-
"""
Flow definition for daily logs materialization.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_smi.materialize_dbt_model.schedules import (
    materialize_smi_flow_schedule,
)
from pipelines.utils.execute_dbt_model.flows import utils_run_dbt_model_flow
from pipelines.utils.utils import set_default_parameters

materialize_smi_flow = deepcopy(utils_run_dbt_model_flow)
materialize_smi_flow.name = "SMI:  - dbt - materializa tabelas da SMI"
materialize_smi_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
materialize_smi_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMI_AGENT_LABEL.value,
    ],
)

materialize_smi_flow_default_parameters = {
    "dataset_id": "infraestrutura_siscob_obras_dashboard",
    "table_id": "obra",
    "mode": "prod",
}
materialize_smi_flow = set_default_parameters(
    materialize_smi_flow, default_parameters=materialize_smi_flow_default_parameters
)

materialize_smi_flow.schedule = materialize_smi_flow_schedule
