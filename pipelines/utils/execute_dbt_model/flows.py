# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import (
    get_k8s_dbt_client,
    is_running_at_datario,
    is_valid_dictionary,
    merge_dictionaries,
    model_parameters_from_secrets,
    run_dbt_model,
)
from pipelines.utils.tasks import (
    get_current_flow_labels,
    log_task,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
) as utils_run_dbt_model_flow:
    # Parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    upstream = Parameter("upstream", default=None, required=False)
    downstream = Parameter("downstream", default=None, required=False)
    exclude = Parameter("exclude", default=None, required=False)
    flags = Parameter("flags", default=None, required=False)
    mode = Parameter("mode", default="dev", required=False)
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    dbt_model_parameters = Parameter("dbt_model_parameters", default={}, required=False)
    dbt_model_secret_parameters = Parameter(
        "dbt_model_secret_parameters", default={}, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

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

    # Parse model parameters
    public_model_parameters = is_valid_dictionary(dbt_model_parameters)

    # Get secret model parameters
    secret_model_parameters = model_parameters_from_secrets(dbt_model_secret_parameters)

    # Merge parameters
    model_parameters = merge_dictionaries(
        dict1=public_model_parameters, dict2=secret_model_parameters
    )

    # Run DBT model
    materialize_this = run_dbt_model(  # pylint: disable=invalid-name
        dbt_client=dbt_client,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        upstream=upstream,
        downstream=downstream,
        exclude=exclude,
        flags=flags,
        _vars=model_parameters,
        sync=True,
    )

    with case(materialize_to_datario, True):
        current_labels = get_current_flow_labels()
        running_at_datario = is_running_at_datario(current_labels)
        with case(running_at_datario, True):
            log_warning = log_task(  # pylint: disable=invalid-name
                "You're running this flow at the datario agent, "
                "I won't submit any more flow runs."
            )
            log_warning.set_upstream(running_at_datario)
        with case(running_at_datario, False):
            datario_materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": "prod",
                    "materialize_to_datario": False,  # setting this to true might cause an
                    # infinite loop
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

utils_run_dbt_model_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
utils_run_dbt_model_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
