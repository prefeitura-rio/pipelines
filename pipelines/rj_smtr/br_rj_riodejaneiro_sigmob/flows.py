# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_sigmob
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import (
    get_k8s_dbt_client,
)

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

from pipelines.rj_smtr.schedules import every_day
from pipelines.rj_smtr.tasks import (
    bq_upload_from_dict,
    build_incremental_model,
    run_dbt_model,
    # , get_local_dbt_client
)

from pipelines.rj_smtr.br_rj_riodejaneiro_sigmob.tasks import request_data

# Flows #

with Flow(
    "SMTR: SIGMOB - Materialização",
    code_owners=["caio", "fernanda"],
) as materialize_sigmob:

    # Get default parameters #
    dataset_id = Parameter("dataset_id", default=constants.SIGMOB_DATASET_ID.value)
    backfill = Parameter("backfill", default=False)

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode="prod")
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Run materialization #
    with case(backfill, True):
        RUN = run_dbt_model(
            dbt_client=dbt_client,
            model=dataset_id,
            flags="--full-refresh",
        )
        INCREMENTAL_RUN = build_incremental_model(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            base_table_id="shapes",
            mat_table_id="shapes_geom",
            wait=RUN,
        )
        LAST_RUN = run_dbt_model(
            dbt_client=dbt_client,
            model="data_versao_efetiva",
            flags="--full-refresh",
            wait=INCREMENTAL_RUN,
        )
        # TESTS = run_dbt_model(
        #     command="test", dbt_client=dbt_client, model=dataset_id, wait=LAST_RUN
        # )
    with case(backfill, False):
        RUN = run_dbt_model(
            dbt_client=dbt_client,
            model=dataset_id,
        )
        # TESTS = run_dbt_model(
        #     command="test", dbt_client=dbt_client, model=dataset_id, wait=RUN
        # )

    materialize_sigmob.set_dependencies(
        task=LAST_RUN, upstream_tasks=[dbt_client, RUN, INCREMENTAL_RUN]
    )


with Flow(
    "SMTR: SIGMOB - Captura",
    code_owners=["caio", "fernanda"],
) as captura_sigmob:

    # Get default parameters #
    endpoints = Parameter("endpoints", default=constants.SIGMOB_ENDPOINTS.value)
    dataset_id = Parameter("dataset_id", default=constants.SIGMOB_DATASET_ID.value)
    materialize = Parameter("materialize", default=True)

    # Run tasks #
    paths_dict = request_data(endpoints=endpoints)
    bq_upload = bq_upload_from_dict(paths=paths_dict, dataset_id=dataset_id)

    # Run materialization #
    with case(materialize, True):
        materialize_run = create_flow_run(
            flow_name=materialize_sigmob.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
            run_name=materialize_sigmob.name,
        )
        materialize_run.set_upstream(bq_upload)

    wait_for_materialization = wait_for_flow_run(
        materialize_run,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )


materialize_sigmob.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
materialize_sigmob.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
materialize_sigmob.schedule = every_day


captura_sigmob.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_sigmob.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# captura_sigmob.schedule = every_day
