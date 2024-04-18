# -*- coding: utf-8 -*-
"""
Flows for gtfs
"""
from copy import deepcopy
from datetime import timedelta

# Imports #

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefect import Parameter, case, task
from prefect.tasks.control_flow import merge


# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.utils import set_default_parameters
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_current_flow_labels,
)

# SMTR Imports #
from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.tasks import get_current_timestamp, get_scheduled_start_times

from pipelines.rj_smtr.flows import default_capture_flow, default_materialization_flow

# SETUP dos Flows

gtfs_captura = deepcopy(default_capture_flow)
gtfs_captura.name = "SMTR: GTFS - Captura (subflow)"
gtfs_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
gtfs_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
gtfs_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
gtfs_captura = set_default_parameters(
    flow=gtfs_captura,
    default_parameters=constants.GTFS_GENERAL_CAPTURE_PARAMS.value,
)

gtfs_materializacao = deepcopy(default_materialization_flow)
gtfs_materializacao.name = "SMTR: GTFS - Materialização (subflow)"
gtfs_materializacao.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
gtfs_materializacao.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
gtfs_materializacao = set_default_parameters(
    flow=gtfs_materializacao,
    default_parameters=constants.GTFS_MATERIALIZACAO_PARAMS.value,
)

with Flow(
    "SMTR: GTFS - Captura/Tratamento",
    code_owners=["rodrigo", "carolinagomes"],
) as gtfs_captura_tratamento:
    # SETUP
    data_versao_gtfs = Parameter("data_versao_gtfs", default=None)
    capture = Parameter("capture", default=True)
    materialize = Parameter("materialize", default=True)

    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=gtfs_captura_tratamento.name + " " + data_versao_gtfs + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()

    with case(capture, True):
        gtfs_capture_parameters = [
            {"timestamp": data_versao_gtfs, **d}
            for d in constants.GTFS_TABLE_CAPTURE_PARAMS.value
        ]

        run_captura = create_flow_run.map(
            flow_name=unmapped(gtfs_captura.name),
            # project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            project_name=unmapped("staging"),
            parameters=gtfs_capture_parameters,
            labels=unmapped(LABELS),
            scheduled_start_time=get_scheduled_start_times(
                timestamp=timestamp,
                parameters=gtfs_capture_parameters,
                intervals={"agency": timedelta(minutes=11)},
            ),
        )

        wait_captura_true = wait_for_flow_run.map(
            run_captura,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

    with case(capture, False):
        wait_captura_false = task(
            lambda: [None], checkpoint=False, name="assign_none_to_previous_runs"
        )()

    wait_captura = merge(wait_captura_true, wait_captura_false)

    with case(materialize, True):
        dbt_vars = {
            "dbt_vars": {
                "data_versao_gtfs": data_versao_gtfs,
                "version": {},
            },
        }

        gtfs_materializacao_parameters = dbt_vars
        gtfs_materializacao_parameters_new = {
            "dataset_id": "gtfs",
        } | dbt_vars

        run_materializacao = create_flow_run(
            flow_name=gtfs_materializacao.name,
            project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            parameters=gtfs_materializacao_parameters,
            labels=LABELS,
            upstream_tasks=[wait_captura],
        )

        run_materializacao_new_dataset_id = create_flow_run(
            flow_name=gtfs_materializacao.name,
            project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            parameters=gtfs_materializacao_parameters_new,
            labels=LABELS,
            upstream_tasks=[wait_captura],
        )

        wait_materializacao = wait_for_flow_run(
            run_materializacao,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        wait_materializacao_new_dataset_id = wait_for_flow_run(
            run_materializacao_new_dataset_id,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

gtfs_captura_tratamento.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
gtfs_captura_tratamento.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
