# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from pipelines.constants import constants
from pipelines.rj_smfp.smfp_egpweb_refresh_data.schedules import (
    smfp_egpweb_monthly_update_schedule,
)
from pipelines.utils.execute_dbt_model.flows import run_dbt_model_flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

run_dbt_smfp_egpweb_flow = deepcopy(run_dbt_model_flow)
run_dbt_smfp_egpweb_flow.name = "SMFP: EGPWEB_Acordo_Resultados - Materializar tabelas"
run_dbt_smfp_egpweb_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_smfp_egpweb_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
run_dbt_smfp_egpweb_flow.schedule = smfp_egpweb_monthly_update_schedule
