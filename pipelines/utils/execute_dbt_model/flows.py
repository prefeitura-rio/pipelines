# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from functools import partial

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.tasks import (
    get_k8s_dbt_client,
    run_dbt_model,
)

from pipelines.utils.utils import notify_discord_on_failure

with Flow(
    name="EMD: template - Executa DBT model",
    on_failure=partial(
        notify_discord_on_failure,
        secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value,
    ),
) as run_dbt_model_flow:

    # Parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    mode = Parameter("mode", default="dev")

    # Get DBT client
    dbt_client = get_k8s_dbt_client(mode=mode)

    # Run DBT model
    run_dbt_model(
        dbt_client=dbt_client,
        dataset_id=dataset_id,
        table_id=table_id,
        sync=True,
    )

run_dbt_model_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_model_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
