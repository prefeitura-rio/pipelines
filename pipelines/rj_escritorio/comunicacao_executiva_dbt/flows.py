# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.comunicacao_executiva_dbt.schedules import (
    comunicacao_executiva_daily_update_schedule,
)
from pipelines.utils.execute_dbt_model.flows import utils_run_dbt_model_flow

run_dbt_comunicacao_executiva_flow = deepcopy(utils_run_dbt_model_flow)
run_dbt_comunicacao_executiva_flow.name = (
    "EMD: Comunicação Executiva - Materializar tabelas"
)
run_dbt_comunicacao_executiva_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_comunicacao_executiva_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
run_dbt_comunicacao_executiva_flow.schedule = (
    comunicacao_executiva_daily_update_schedule
)
