# -*- coding: utf-8 -*-
"""
Database dumping flows for sme project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_smtr.materialize_to_datario.schedules import (
    smtr_materialize_to_datario_daily_schedule,
)
from pipelines.utils.execute_dbt_model.flows import utils_run_dbt_model_flow
from pipelines.utils.utils import set_default_parameters

# # VIAGEM SPPO #

smtr_materialize_to_datario_viagem_sppo_flow = deepcopy(utils_run_dbt_model_flow)

smtr_materialize_to_datario_viagem_sppo_flow.name = (
    "SMTR: Viagens SPPO - Publicação `datario`"
)
smtr_materialize_to_datario_viagem_sppo_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
smtr_materialize_to_datario_viagem_sppo_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_DATARIO_DEV_AGENT_LABEL.value,  # TEST
    ],
)

publish_viagem_sppo_default_parameters = {
    "dataset_id": "transporte_rodoviario_municipal",
    "table_id": "viagem_onibus",
    "mode": "prod",
}

smtr_materialize_to_datario_viagem_sppo_flow = set_default_parameters(
    smtr_materialize_to_datario_viagem_sppo_flow,
    default_parameters=publish_viagem_sppo_default_parameters,
)

# # DAILY SCHEDULED DATA #

smtr_materialize_to_datario_daily_flow = deepcopy(utils_run_dbt_model_flow)

smtr_materialize_to_datario_daily_flow.name = (
    "SMTR: Dados diários - Publicação `datario`"
)
smtr_materialize_to_datario_daily_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
smtr_materialize_to_datario_daily_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_DATARIO_DEV_AGENT_LABEL.value,  # TEST
    ],
)
smtr_materialize_to_datario_daily_flow.schedule = (
    smtr_materialize_to_datario_daily_schedule
)
