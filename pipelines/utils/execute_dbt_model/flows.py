# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from functools import partial

from prefect import Flow, Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.execute_dbt_model.tasks import (
    get_k8s_dbt_client,
    run_dbt_model,
)
from pipelines.utils.tasks import rename_current_flow_run_dataset_table

from pipelines.utils.utils import notify_discord_on_failure

with Flow(
    name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
    on_failure=partial(
        notify_discord_on_failure,
        secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value,
    ),
) as run_dbt_model_flow:

    # Parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    mode = Parameter("mode", default="dev", required=False)
    materialize_to_datario = Parameter("materialize_to_datario", default=False, required=False)

    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Materialize: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    # Get DBT client
    dbt_client = get_k8s_dbt_client(mode=mode, wait=rename_flow_run)

    # Run DBT model
    materialize_this = run_dbt_model(
        dbt_client=dbt_client,
        dataset_id=dataset_id,
        table_id=table_id,
        sync=True,
    )

    with case(materialize_to_datario):
        datario_materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
            },
            labels=[
                constants.RJ_DATARIO_AGENT_LABEL.value,
            ],
            run_name=f"Publish to datario: {dataset_id}.{table_id}",
        )
        datario_materialization_flow.set_upstream(materialize_this)

        wait_for_materialization = wait_for_flow_run(
            datario_materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

run_dbt_model_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_model_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
