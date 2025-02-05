# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_smac.materialize_povo_comunidades_tradicionais.schedules import (
#     materialize_povo_comunidades_tradicionais_schedule,
# )
from pipelines.utils.execute_dbt_model.flows import utils_run_dbt_model_flow

materialize_povo_comunidades_tradicionais_flow = deepcopy(utils_run_dbt_model_flow)
materialize_povo_comunidades_tradicionais_flow.name = (
    "SMAC: povo_comunidades_tradicionais - Materializar tabelas"
)
materialize_povo_comunidades_tradicionais_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
materialize_povo_comunidades_tradicionais_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMAC_AGENT_LABEL.value,
    ],
)
# materialize_povo_comunidades_tradicionais_flow.schedule = (
#     materialize_povo_comunidades_tradicionais_schedule
# )
