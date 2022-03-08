"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.flows import run_dbt_model_flow
from pipelines.rj_segovi.execute_dbt_model.schedules import (
    _1746_monthly_update_schedule,
)

run_dbt_1746_flow = deepcopy(run_dbt_model_flow)
run_dbt_1746_flow.name = "SEGOVI: 1746 - Materializar tabelas"
run_dbt_1746_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_1746_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
run_dbt_1746_flow.schedule = _1746_monthly_update_schedule
