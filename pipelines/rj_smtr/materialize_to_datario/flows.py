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
from pipelines.utils.execute_dbt_model.flows import run_dbt_model_flow

materialize_smtr_flow = deepcopy(run_dbt_model_flow)
materialize_smtr_flow.name = "SMTR: Materializar dados para publicação no `datario`"
materialize_smtr_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
materialize_smtr_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_DATARIO_AGENT_LABEL.value,
    ],
)
materialize_smtr_flow.schedule = smtr_materialize_to_datario_daily_schedule
