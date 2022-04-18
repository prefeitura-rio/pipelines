# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from pipelines.constants import constants
from pipelines.rj_escritorio.seconserva_buracos_refresh_data.schedules import (
    seconserva_buracos_daily_update_schedule,
)
from pipelines.utils.execute_dbt_model.flows import run_dbt_model_flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

run_dbt_seconserva_buracos_flow = deepcopy(run_dbt_model_flow)
run_dbt_seconserva_buracos_flow.name = "EMD: seconserva_buracos - Materializar tabelas"
run_dbt_seconserva_buracos_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_seconserva_buracos_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
run_dbt_seconserva_buracos_flow.schedule = seconserva_buracos_daily_update_schedule
