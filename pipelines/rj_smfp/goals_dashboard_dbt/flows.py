# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_smfp.goals_dashboard_dbt.schedules import (
#     smfp_dashboard_metas_daily_update_schedule,
# )
from pipelines.utils.execute_dbt_model.flows import utils_run_dbt_model_flow
from pipelines.utils.utils import set_default_parameters

run_dbt_smfp_dashboard_metas_flow = deepcopy(utils_run_dbt_model_flow)
run_dbt_smfp_dashboard_metas_flow.name = (
    "SMFP: Dashboard de Metas - Materializar tabelas"
)
run_dbt_smfp_dashboard_metas_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_smfp_dashboard_metas_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)

smfp_dashboard_metas_default_parameters = {
    "dataset_id": "planejamento_gestao_dashboard_metas",
    "upstream": True,
    "materialize_to_datario": False,
}
run_dbt_smfp_dashboard_metas_flow = set_default_parameters(
    run_dbt_smfp_dashboard_metas_flow,
    default_parameters=smfp_dashboard_metas_default_parameters,
)

# run_dbt_smfp_dashboard_metas_flow.schedule = smfp_dashboard_metas_daily_update_schedule
