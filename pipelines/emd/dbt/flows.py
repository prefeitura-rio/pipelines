"""
DBT-related flows.
"""

from copy import deepcopy

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.emd.dbt.tasks import (
    get_k8s_dbt_client,
    run_dbt_model,
)
from pipelines.emd.dbt.schedules import (
    _1746_monthly_update_schedule,
    ergon_monthly_update_schedule,
    sme_monthly_update_schedule,
)

with Flow("EMD: template - Executa DBT model") as run_dbt_model_flow:

    # Parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    mode = Parameter("mode", default="dev")

    # Get DBT client
    dbt_client = get_k8s_dbt_client(mode=mode)

    # Run DBT model
    run_dbt_model(
        dbt_client=dbt_client, dataset_id=dataset_id, table_id=table_id, sync=True
    )

run_dbt_model_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_model_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

run_dbt_ergon_flow = deepcopy(run_dbt_model_flow)
run_dbt_ergon_flow.name = "EMD: ergon - Materializar tabelas Ergon"
run_dbt_ergon_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_ergon_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
run_dbt_ergon_flow.schedule = ergon_monthly_update_schedule

run_dbt_sme_flow = deepcopy(run_dbt_model_flow)
run_dbt_sme_flow.name = "SME: educacao_basica - Materializar tabelas"
run_dbt_sme_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_sme_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
run_dbt_sme_flow.schedule = sme_monthly_update_schedule

run_dbt_1746_flow = deepcopy(run_dbt_model_flow)
run_dbt_1746_flow.name = "SEOP: 1746 - Materializar tabelas"
run_dbt_1746_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_1746_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
run_dbt_1746_flow.schedule = _1746_monthly_update_schedule
