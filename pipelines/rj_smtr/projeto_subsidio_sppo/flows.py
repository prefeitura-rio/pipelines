# -*- coding: utf-8 -*-
"""
Flows for projeto_subsidio_sppo
"""

from prefect import Parameter, case
from prefect.tasks.control_flow import merge
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# EMD Imports #

from pipelines.constants import constants
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_now_date,
    get_current_flow_mode,
    get_current_flow_labels,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

# SMTR Imports #

from pipelines.rj_smtr.constants import constants as smtr_constants

from pipelines.rj_smtr.tasks import (
    fetch_dataset_sha,
    # get_materialization_date_range,
    # get_local_dbt_client,
    # set_last_run_timestamp,
)

from pipelines.rj_smtr.schedules import (
    every_day_hour_five,
)
from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

# Flows #

with Flow(
    "SMTR: Viagens SPPO - Pré-produção: ",
    code_owners=["rodrigo", "fernanda"],
) as subsidio_sppo_preprod:

    # Rename flow run
    current_date = get_now_date()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="SMTR - Viagens SPPO (preprod): ", now_time=current_date
    )

    # Get default parameters #
    run_date = Parameter("run_date", default=False)

    with case(run_date, False):
        param_date = current_date
    with case(run_date, not False):
        default_date = run_date

    run_date = merge(param_date, default_date)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    dataset_sha = fetch_dataset_sha(
        dataset_id=smtr_constants.SUBSIDIO_SPPO_DATASET_ID.value,
    )

    RUN = run_dbt_model(
        dbt_client=dbt_client,
        dataset_id=smtr_constants.SUBSIDIO_SPPO_DATASET_ID.value,
        _vars={"run_date": run_date},
    )

subsidio_sppo_preprod.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_preprod.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value, labels=[constants.RJ_SMTR_DEV_AGENT_LABEL.value]
)

subsidio_sppo_preprod.schedule = every_day_hour_five
