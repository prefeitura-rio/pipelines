# -*- coding: utf-8 -*-
"""
Flows for gtfs
"""
from copy import deepcopy

# Imports #

# from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS


# EMD Imports #

# from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.constants import constants as constants_emd
from pipelines.utils.decorators import Flow

# SMTR Imports #
from pipelines.rj_smtr.constants import constants

# from pipelines.rj_smtr.br_rj_riodejaneiro_gtfs.tasks import (
#    download_gtfs,
#    get_current_timestamp_from_date,
# )


from pipelines.rj_smtr.flows import default_capture_flow, default_materialization_flow


# FLOW 2: Captura e aninhamento do dado

# Bucket:
# - raw: txt na particao correta
# - staging: csv aninhado na particao correta

# BILHETAGEM PRINCIPAL - CAPTURA DIÁRIA DE DIVERSAS TABELAS #
# gtfs_captura = deepcopy(default_capture_flow)
# gtfs_captura.name = "SMTR: GTFS (captura)"
# gtfs_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
# gtfs_captura.run_config = KubernetesRun(
#     image=emd_constants.DOCKER_IMAGE.value,
#     labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
# )
# gtfs_captura.schedule = gtfs_captura_schedule


gtfs_materializacao = deepcopy(default_materialization_flow)
gtfs_materializacao.name = "SMTR - Materialização dos dados do GTFS"
gtfs_materializacao.storage = GCS(constants_emd.GCS_FLOWS_BUCKET.value)
gtfs_materializacao.run_config = KubernetesRun(
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

gtfs_materializacao_parameters = {
    "dataset_id": constants.GTFS_DATASET_ID.value,
    "dbt_vars": constants.GTFS_MATERIALIZACAO_PARAMS.value,
}

with Flow(
    "SMTR: GTFS - Captura/Tratamento",
    code_owners=["rodrigo", "carol"],
) as gtfs_captura:
    # SETUP

    gtfs_captura = deepcopy(default_capture_flow)
    gtfs_captura.name = "SMTR: GTFS - Captura/Tratamento"
    gtfs_captura.storage = GCS(constants_emd.GCS_FLOWS_BUCKET.value)
    gtfs_captura.run_config = KubernetesRun(
        image=constants_emd.DOCKER_IMAGE.value,
        labels=[constants_emd.RJ_SMTR_DEV_AGENT_LABEL.value],
    )


"""
    with case(download_gtfs, True):
        parameters = {
            "date": Parameter("date", default=None),
            "feed_start_date": Parameter("feed_start_date", default=None),
            "feed_end_date": Parameter("feed_end_date", default=None),
        }
        GTFS_CAPTURA_RUN = create_flow_run(
            flow_name=download_gtfs.name,
            project_name=constants.GTFS_DATASET_ID.value,
            run_name=download_gtfs.name,
            parameters=parameters,
        )
        GTFS_CAPTURA_WAIT = wait_for_flow_run(
            GTFS_CAPTURA_RUN,
            stream_logs=True,
            stream_states=True,
        )
    """
# download_gtfs.schedule = None

# gtfs_captura.schedule = None
