# -*- coding: utf-8 -*-
"""
Flows for projeto_subsidio_sppo
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# EMD Imports #

from pipelines.constants import emd_constants
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_now_day,
    get_current_flow_mode,
    get_current_flow_labels,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

# SMTR Imports #

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.tasks import (
    fetch_dataset_sha,
    # get_local_dbt_client,
    run_dbt_model,
)
from pipelines.rj_smtr.schedules import (
    every_day_hour_six_dev,
)

# Flows #

with Flow("SMTR - Subsídio SPPO", code_owners=["caio", "fernanda"]) as subsidio_sppo:
    # Get default parameters #
    run_date = Parameter("run_date", default=get_now_day())
    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix="Subsídio SPPO: ", now_time=run_date
    )

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Set specific run parameters #
    dataset_sha = fetch_dataset_sha(
        dataset_id=constants.SUBSIDIO_SPPO_DATASET_ID.value,
    )

    # Run materialization #
    RUN = run_dbt_model(
        dbt_client=dbt_client,
        model=constants.SUBSIDIO_SPPO_DATASET_ID.value,
        upstream=False,
        exclude="+viagem_planejada",
        _vars=[run_date, dataset_sha],
    )

subsidio_sppo.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
subsidio_sppo.schedule = every_day_hour_six_dev
