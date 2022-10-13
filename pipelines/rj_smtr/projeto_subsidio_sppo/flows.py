# -*- coding: utf-8 -*-
"""
Flows for projeto_subsidio_sppo
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefect.utilities.edges import unmapped


# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.projeto_subsidio_sppo.tasks import (
    get_date_range,
    get_max_complete_date,
    get_run_params,
    query_max_planned_date,
)
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
    bq_upload,
    fetch_dataset_sha,
    get_bool,
    query_and_save_csv_by_date,
    set_last_run_timestamp,
    # get_local_dbt_client,
)
from pipelines.utils.execute_dbt_model.tasks import run_dbt_model
from pipelines.rj_smtr.schedules import (
    every_day_hour_six,
)

# Flows #
with Flow("SMTR - Subsidio Sensor") as subsidio_sensor:

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)
    dbt_client = get_k8s_dbt_client(mode=MODE)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)
    dataset_id = Parameter(
        "dataset_id", default=constants.SUBSIDIO_SPPO_DATASET_ID.value
    )
    table_id = Parameter(
        "complete_trips_table_id",
        default=constants.SUBSIDIO_SPPO_COMPLETED_TABLE_ID.value,
    )
    # SETUP RUN DATES
    max_complete_date = get_max_complete_date()
    max_planned_date = query_max_planned_date(last_run_date=max_complete_date)

    date_range = get_date_range(
        max_planned_date=max_planned_date, max_complete_date=max_complete_date
    )
    dataset_sha = fetch_dataset_sha(dataset_id)

    with case(get_bool(date_range), True):
        run_params = get_run_params(date_range=date_range, dataset_sha=dataset_sha)
        # RUN MODEL AND SET TS ON REDIS
        RUN = run_dbt_model.map(
            dbt_client=unmapped(dbt_client),
            model=unmapped(dataset_id),
            upstream=unmapped(False),
            exclude=unmapped("+viagem_planejada"),
            _vars=run_params,
        )
        SET_LAST = set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_range[-1],
            wait=RUN,
        )
        # SAVE CSV SNAPSHOT AND UPLOAD
        local_paths, partitions = query_and_save_csv_by_date.map(
            dataset_id=unmapped(dataset_id),
            table_id=unmapped(table_id),
            date_filter=date_range,
        )
        final_status = bq_upload(
            dataset_id=unmapped(dataset_id),
            table_id=unmapped(table_id),
            filepath=local_paths,
            partitions=partitions,
            status=unmapped({"error": None}),
        )
with Flow(
    "SMTR - Subsídio SPPO - Viagens", code_owners=["caio", "fernanda"]
) as subsidio_sppo_viagens:
    # Get run parameters #
    run_date = Parameter("run_date", default=False)

    with case(run_date, False):
        default_date = get_now_day()
    with case(run_date, not False):
        param_date = run_date

    run_date = merge(default_date, param_date)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix="Subsídio SPPO - Viagens: ", now_time=run_date
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
        _vars=[{"run_date": run_date}, dataset_sha],
    )

subsidio_sppo_viagens.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_viagens.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
subsidio_sppo_viagens.schedule = every_day_hour_six


with Flow(
    "SMTR - Subsídio SPPO - Planejado", code_owners=["caio", "fernanda"]
) as subsidio_sppo_planejado:
    # Get default parameters #
    shapes_version = Parameter("shapes_version", default="2022-08-01")
    frequencies_version = Parameter("frequencies_version", default="2022-08-01")
    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix="Subsídio SPPO - Planejado: ", now_time=frequencies_version
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
        model="viagem_planejada",
        upstream=True,
        exclude="br_rj_riodejaneiro_sigmob",
        _vars=[
            {"shapes_version": shapes_version},
            {"frequencies_version": frequencies_version},
            dataset_sha,
        ],
    )

subsidio_sppo_planejado.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_planejado.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
