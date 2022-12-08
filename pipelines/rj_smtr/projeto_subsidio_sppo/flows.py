# -*- coding: utf-8 -*-
"""
Flows for projeto_subsidio_sppo
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped

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
    # get_local_dbt_client,
    # set_last_run_timestamp,
)

from pipelines.rj_smtr.materialize_to_datario.flows import (
    smtr_materialize_to_datario_viagem_sppo_flow,
)

from pipelines.rj_smtr.schedules import (
    every_day_hour_five,
    every_dayofmonth_one_and_sixteen,
)
from pipelines.utils.execute_dbt_model.tasks import run_dbt_model
from pipelines.rj_smtr.projeto_subsidio_sppo.tasks import get_run_dates

# Flows #

with Flow(
    "SMTR: Viagens SPPO",
    code_owners=["rodrigo", "fernanda"],
) as subsidio_sppo_preprod:

    # Rename flow run
    current_date = get_now_date()

    # Get default parameters #
    date_range_start = Parameter("date_range_start", default=False)
    date_range_end = Parameter("date_range_end", default=False)

    run_date = get_run_dates(date_range_start, date_range_end)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="SMTR - Viagens SPPO: ", now_time=run_date
    )

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    dataset_sha = fetch_dataset_sha(
        dataset_id=smtr_constants.SUBSIDIO_SPPO_DATASET_ID.value,
    )

    RUN = run_dbt_model.map(
        dbt_client=unmapped(dbt_client),
        dataset_id=unmapped(smtr_constants.SUBSIDIO_SPPO_DATASET_ID.value),
        _vars=run_date,
    )

subsidio_sppo_preprod.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_preprod.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value, labels=[constants.RJ_SMTR_AGENT_LABEL.value]
)

subsidio_sppo_preprod.schedule = every_day_hour_five

with Flow(
    "SMTR: Subsídio SPPO Apuração",
    code_owners=["rodrigo", "fernanda"],
) as subsidio_sppo_apuracao:

    # Rename flow run
    current_date = get_now_date()

    # Get default parameters #
    input_date = Parameter("run_date", default=False)

    run_date = get_run_dates(input_date)[0]

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="SMTR - Subsídio SPPO Apuração: ", now_time=current_date
    )

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    dataset_sha = fetch_dataset_sha(
        dataset_id=smtr_constants.SUBSIDIO_SPPO_DASHBOAD_DATASET_ID.value,
    )

    RUN = run_dbt_model(
        dbt_client=dbt_client,
        dataset_id=smtr_constants.SUBSIDIO_SPPO_DASHBOAD_DATASET_ID.value,
        _vars=run_date,
    )

    run_materialize = create_flow_run(
        flow_name=smtr_materialize_to_datario_viagem_sppo_flow.name,
        project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        labels=[
            constants.RJ_DATARIO_AGENT_LABEL.value,
        ],
        run_name=smtr_materialize_to_datario_viagem_sppo_flow.name,
        parameters={
            "dataset_id": "transporte_rodoviario_municipal",
            "table_id": "viagem_onibus",
            "mode": "prod",
            "dbt_model_parameters": run_date,
        },
    )

    wait_materialize = wait_for_flow_run(
        run_materialize,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

subsidio_sppo_apuracao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_apuracao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value, labels=[constants.RJ_SMTR_DEV_AGENT_LABEL.value]
)

subsidio_sppo_apuracao.schedule = every_dayofmonth_one_and_sixteen
