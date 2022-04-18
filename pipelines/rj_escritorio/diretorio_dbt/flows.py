# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.flows import run_dbt_model_flow
from pipelines.rj_escritorio.diretorio_dbt.schedules import (
    diretorio_weekly_update_schedule,
)

run_dbt_diretorio_flow = deepcopy(run_dbt_model_flow)
run_dbt_diretorio_flow.name = "EMD: diretorio - Materializar tabelas DBT"
run_dbt_diretorio_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_diretorio_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
run_dbt_diretorio_flow.schedule = diretorio_weekly_update_schedule
