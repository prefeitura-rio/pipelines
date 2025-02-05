# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_smi.dbt_obras_dashboard.schedules import (
#     smi_dashboard_obras_monthly_update_schedule,
# )
from pipelines.utils.execute_dbt_model.flows import utils_run_dbt_model_flow
from pipelines.utils.utils import set_default_parameters

run_dbt_smi_dashboard_obras_flow = deepcopy(utils_run_dbt_model_flow)
run_dbt_smi_dashboard_obras_flow.name = "SMI: Dashboard de Obras - Materializar tabelas"
run_dbt_smi_dashboard_obras_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_smi_dashboard_obras_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)

smi_dashboard_obras_default_parameters = {
    "dataset_id": "infraestrutura_siscob_obras_dashboard",
    "upstream": True,
    "dbt_alias": True,
}
run_dbt_smi_dashboard_obras_flow = set_default_parameters(
    run_dbt_smi_dashboard_obras_flow,
    default_parameters=smi_dashboard_obras_default_parameters,
)

# run_dbt_smi_dashboard_obras_flow.schedule = smi_dashboard_obras_monthly_update_schedule
