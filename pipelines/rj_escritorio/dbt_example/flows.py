# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.dbt_example.schedules import (
    example_dbt_tables_schedule,
)
from pipelines.utils.execute_dbt_model.flows import utils_run_dbt_model_flow
from pipelines.utils.utils import set_default_parameters

dbt_example_formacao_infra = deepcopy(utils_run_dbt_model_flow)
dbt_example_formacao_infra.name = "EMD: Exemplo Formação DBT - Materializar tabelas"
dbt_example_formacao_infra.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dbt_example_formacao_infra.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)

dbt_example_default_parameters = {
    "dataset_id": "test_formacao",
}
dbt_example_formacao_infra = set_default_parameters(
    dbt_example_formacao_infra,
    default_parameters=dbt_example_default_parameters,
)

dbt_example_formacao_infra.schedule = example_dbt_tables_schedule
