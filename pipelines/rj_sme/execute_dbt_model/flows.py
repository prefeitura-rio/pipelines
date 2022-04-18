# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.flows import run_dbt_model_flow

# from pipelines.rj_sme.execute_dbt_model.schedules import (
#     sme_monthly_update_schedule,
# )

run_dbt_sme_flow = deepcopy(run_dbt_model_flow)
run_dbt_sme_flow.name = "SME: educacao_basica - Materializar tabelas"
run_dbt_sme_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_sme_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# run_dbt_sme_flow.schedule = sme_monthly_update_schedule
