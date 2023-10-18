# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_bilhetagem
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped


# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_current_flow_labels,
)


from pipelines.utils.utils import set_default_parameters

# SMTR Imports #

from pipelines.rj_smtr.flows import (
    default_capture_flow,
    default_materialization_flow,
)

from pipelines.rj_smtr.tasks import get_current_timestamp

from pipelines.rj_smtr.constants import constants

from pipelines.rj_smtr.schedules import every_hour, every_minute


GENERAL_CAPTURE_DEFAULT_PARAMS = {
    "dataset_id": constants.BILHETAGEM_DATASET_ID.value,
    "secret_path": constants.BILHETAGEM_SECRET_PATH.value,
    "source_type": constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value["source_type"],
}

# Flows #

# BILHETAGEM TRANSAÇÃO - CAPTURA A CADA MINUTO #

bilhetagem_transacao_captura = deepcopy(default_capture_flow)
bilhetagem_transacao_captura.name = "SMTR: Bilhetagem Transação - Captura"
bilhetagem_transacao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_transacao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

bilhetagem_transacao_captura = set_default_parameters(
    flow=bilhetagem_transacao_captura,
    default_parameters=GENERAL_CAPTURE_DEFAULT_PARAMS
    | constants.BILHETAGEM_TRANSACAO_CAPTURE_PARAMS.value,
)

bilhetagem_transacao_captura.schedule = every_minute


# BILHETAGEM AUXILIAR - SUBFLOW PARA RODAR ANTES DE CADA MATERIALIZAÇÃO #

bilhetagem_auxiliar_captura = deepcopy(default_capture_flow)
bilhetagem_auxiliar_captura.name = "SMTR: Bilhetagem Auxiliar - Captura (subflow)"
bilhetagem_auxiliar_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_auxiliar_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

bilhetagem_auxiliar_captura = set_default_parameters(
    flow=bilhetagem_auxiliar_captura,
    default_parameters=GENERAL_CAPTURE_DEFAULT_PARAMS,
)

# MATERIALIZAÇÃO - SUBFLOW DE MATERIALIZAÇÃO
bilhetagem_materializacao = deepcopy(default_materialization_flow)
bilhetagem_materializacao.name = "SMTR: Bilhetagem Transação - Materialização (subflow)"
bilhetagem_materializacao.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_materializacao.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

bilhetagem_materializacao_parameters = {
    "dataset_id": constants.BILHETAGEM_DATASET_ID.value
} | constants.BILHETAGEM_MATERIALIZACAO_PARAMS.value

bilhetagem_materializacao = set_default_parameters(
    flow=bilhetagem_materializacao,
    default_parameters=bilhetagem_materializacao_parameters,
)

# RECAPTURA

bilhetagem_recaptura = deepcopy(default_capture_flow)
bilhetagem_recaptura.name = "SMTR: Bilhetagem - Recaptura (subflow)"
bilhetagem_recaptura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_recaptura = set_default_parameters(
    flow=bilhetagem_recaptura,
    default_parameters=GENERAL_CAPTURE_DEFAULT_PARAMS | {"recapture": True},
)

# TRATAMENTO - RODA DE HORA EM HORA, RECAPTURAS + CAPTURA AUXILIAR + MATERIALIZAÇÃO
with Flow(
    "SMTR: Bilhetagem Transação - Tratamento",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as bilhetagem_transacao_tratamento:
    # Configuração #

    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=bilhetagem_transacao_tratamento.name + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()

    # Recapturas

    run_recaptura_trasacao = create_flow_run(
        flow_name=bilhetagem_recaptura.name,
        # project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
        project_name="staging",
        labels=LABELS,
        parameters=constants.BILHETAGEM_TRANSACAO_CAPTURE_PARAMS.value,
    )

    wait_recaptura_trasacao = wait_for_flow_run(
        run_recaptura_trasacao,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    runs_recaptura_auxiliar = create_flow_run.map(
        flow_name=unmapped(bilhetagem_recaptura.name),
        # project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
        project_name=unmapped("staging"),
        parameters=constants.BILHETAGEM_CAPTURE_PARAMS.value,
        labels=unmapped(LABELS),
        upstream_tasks=unmapped([wait_recaptura_trasacao]),
    )

    wait_recaptura_auxiliar = wait_for_flow_run.map(
        runs_recaptura_auxiliar,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

    # Captura
    runs_captura = create_flow_run.map(
        flow_name=unmapped(bilhetagem_auxiliar_captura.name),
        # project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
        project_name=unmapped("staging"),
        parameters=constants.BILHETAGEM_CAPTURE_PARAMS.value,
        labels=unmapped(LABELS),
        upstream_tasks=unmapped([wait_recaptura_auxiliar]),
    )

    wait_captura = wait_for_flow_run.map(
        runs_captura,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

    # Materialização
    # run_materializacao = create_flow_run(
    #     flow_name=bilhetagem_materializacao.name,
    #     # project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
    #     project_name="staging",
    #     labels=LABELS,
    #     upstream_tasks=[wait_captura],
    # )

    # wait_materializacao = wait_for_flow_run(
    #     run_materializacao,
    #     stream_states=True,
    #     stream_logs=True,
    #     raise_final_state=True,
    # )

bilhetagem_transacao_tratamento.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_transacao_tratamento.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
bilhetagem_transacao_tratamento.schedule = every_hour
