# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_setur.dbt_seeketing.schedules import (
    setur_seeketing_daily_update_schedule,
)
from pipelines.utils.execute_dbt_model.flows import utils_run_dbt_model_flow
from pipelines.utils.utils import set_default_parameters

dbt_setur_seeketing = deepcopy(utils_run_dbt_model_flow)
dbt_setur_seeketing.name = (
    "SETUR: Seeketing - Materializar tabelas"
)
dbt_setur_seeketing.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dbt_setur_seeketing.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)

setur_seeketing_default_parameters = {
    "dataset_id": "turismo_fluxo_visitantes",
    "upstream": True,
}
dbt_setur_seeketing = set_default_parameters(
    dbt_setur_seeketing,
    default_parameters=setur_seeketing_default_parameters,
)

dbt_setur_seeketing.schedule = setur_seeketing_daily_update_schedule
