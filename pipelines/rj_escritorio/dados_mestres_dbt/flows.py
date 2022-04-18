# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.flows import run_dbt_model_flow
from pipelines.rj_escritorio.dados_mestres_dbt.schedules import (
    dados_mestresweekly_update_schedule,
)

run_dbt_dados_mestresflow = deepcopy(run_dbt_model_flow)
run_dbt_dados_mestresflow.name = "EMD: dados_mestres - Materializar tabelas DBT"
run_dbt_dados_mestresflow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_dados_mestresflow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
run_dbt_dados_mestresflow.schedule = dados_mestresweekly_update_schedule
