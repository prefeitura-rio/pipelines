# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Vitai heathrecord dumping flows
"""
from datetime import timedelta

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import get_current_flow_labels
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.constants import constants


with Flow(
    name="SMS: DBT Materialize - Materialize Datalake", code_owners=["thiago"]
) as sms_materialize_datalake:
    # Parameters
    # Paramenters for GCP
    dataset_id = Parameter("dataset_id", default="raw_prontuario_vitai", required=False)

    table_id = Parameter("table_id", default="estoque_posicao", required=False)
    # Parameters for materialization
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    materialization_mode = Parameter("mode", default="dev", required=False)

    current_flow_labels = get_current_flow_labels()
    materialization_flow = create_flow_run(
        flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
        project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        parameters={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "mode": materialization_mode,
            "dbt_alias": True,
            "materialize_to_datario": materialize_to_datario,
        },
        labels=current_flow_labels,
        run_name=f"Materialize {dataset_id}.{table_id}",
    )

    wait_for_materialization = wait_for_flow_run(
        materialization_flow,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    wait_for_materialization.max_retries = (
        dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
    )
    wait_for_materialization.retry_delay = timedelta(
        seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
    )


sms_materialize_datalake.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_materialize_datalake.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)
