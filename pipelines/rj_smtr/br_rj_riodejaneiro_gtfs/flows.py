# -*- coding: utf-8 -*-
"""
Flows for gtfs
"""
from copy import deepcopy

# Imports #

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefect import Parameter


# EMD Imports #

from pipelines.constants import constants as constants_emd
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_current_flow_labels,
)

# SMTR Imports #
from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.tasks import (
    get_current_timestamp,
)

from pipelines.rj_smtr.flows import default_capture_flow, default_materialization_flow

# SETUP dos Flows

gtfs_captura = deepcopy(default_capture_flow)
gtfs_captura.name = "SMTR - Captura dos dados do GTFS"
gtfs_captura.storage = GCS(constants_emd.GCS_FLOWS_BUCKET.value)
gtfs_captura.run_config = KubernetesRun(
    image=constants_emd.DOCKER_IMAGE.value,
    labels=[constants_emd.RJ_SMTR_DEV_AGENT_LABEL.value],
)

gtfs_materializacao = deepcopy(default_materialization_flow)
gtfs_materializacao.name = "SMTR - Materialização dos dados do GTFS"
gtfs_materializacao.storage = GCS(constants_emd.GCS_FLOWS_BUCKET.value)
gtfs_materializacao.run_config = KubernetesRun(
    image=constants_emd.DOCKER_IMAGE.value,
    labels=[constants_emd.RJ_SMTR_DEV_AGENT_LABEL.value],
)

with Flow(
    "SMTR: GTFS - Captura/Tratamento",
    code_owners=["rodrigo", "carolinagomes"],
) as gtfs_captura_tratamento:
    # SETUP

    data_versao_gtfs = Parameter("data_versao_gtfs", default=None)

    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=gtfs_captura_tratamento.name + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()

    run_captura = create_flow_run.map(
        flow_name=unmapped(gtfs_captura.name),
        project_name=unmapped("staging"),
        parameters=[constants.GTFS_TABLE_CAPTURE_PARAMS.value],
        labels=unmapped(LABELS),
    )

    wait_captura = wait_for_flow_run.map(
        run_captura,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

    gtfs_materializacao_parameters = {
        "dataset_id": constants.GTFS_DATASET_ID.value,
        "dbt_vars": {"data_versao_gtfs": data_versao_gtfs},
    }

    run_materializacao = create_flow_run(
        flow_name=unmapped(gtfs_materializacao.name),
        project_name=unmapped("staging"),
        parameters=unmapped(gtfs_materializacao_parameters),
        labels=unmapped(LABELS),
        upstream_tasks=[wait_captura],
    )

    wait_materializacao = wait_for_flow_run(
        run_materializacao,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

gtfs_captura_tratamento.storage = GCS(constants_emd.GCS_FLOWS_BUCKET.value)
gtfs_captura_tratamento.run_config = KubernetesRun(
    image=constants_emd.DOCKER_IMAGE.value,
    labels=[constants_emd.RJ_SMTR_DEV_AGENT_LABEL.value],
)
