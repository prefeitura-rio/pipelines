# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_bilhetagem
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefect import Parameter, case, task
from prefect.tasks.control_flow import merge


# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_current_flow_labels,
)


from pipelines.utils.utils import set_default_parameters, skip_if_running_handler

# SMTR Imports #

from pipelines.rj_smtr.flows import (
    default_capture_flow,
    default_materialization_flow,
)

from pipelines.rj_smtr.tasks import get_rounded_timestamp, get_current_timestamp

from pipelines.rj_smtr.constants import constants

from pipelines.rj_smtr.schedules import (
    every_hour,
    every_minute,
    every_day_hour_five,
    # every_day_hour_seven,
    every_5_minutes,
)

# Flows #

# BILHETAGEM TRANSAÇÃO - CAPTURA A CADA MINUTO #

bilhetagem_transacao_captura = deepcopy(default_capture_flow)
bilhetagem_transacao_captura.name = "SMTR: Bilhetagem Transação - Captura"
bilhetagem_transacao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_transacao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_transacao_captura = set_default_parameters(
    flow=bilhetagem_transacao_captura,
    default_parameters=constants.BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS.value
    | constants.BILHETAGEM_TRANSACAO_CAPTURE_PARAMS.value,
)

# bilhetagem_transacao_captura.schedule = every_minute


bilhetagem_transacao_riocard_captura = deepcopy(default_capture_flow)
bilhetagem_transacao_riocard_captura.name = (
    "SMTR: Bilhetagem Transação RioCard - Captura"
)
bilhetagem_transacao_riocard_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_transacao_riocard_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_transacao_riocard_captura = set_default_parameters(
    flow=bilhetagem_transacao_riocard_captura,
    default_parameters=constants.BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS.value
    | constants.BILHETAGEM_TRANSACAO_RIOCARD_CAPTURE_PARAMS.value,
)

# bilhetagem_transacao_riocard_captura.schedule = every_minute

# BILHETAGEM FISCALIZAÇÃO - CAPTURA A CADA 5 MINUTOS #

bilhetagem_fiscalizacao_captura = deepcopy(default_capture_flow)
bilhetagem_fiscalizacao_captura.name = "SMTR: Bilhetagem Fiscalização - Captura"
bilhetagem_fiscalizacao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_fiscalizacao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_fiscalizacao_captura = set_default_parameters(
    flow=bilhetagem_fiscalizacao_captura,
    default_parameters=constants.BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS.value
    | constants.BILHETAGEM_FISCALIZACAO_CAPTURE_PARAMS.value,
)

# bilhetagem_fiscalizacao_captura.schedule = every_5_minutes

# BILHETAGEM INTEGRAÇÃO - CAPTURA A CADA MINUTO #

bilhetagem_integracao_captura = deepcopy(default_capture_flow)
bilhetagem_integracao_captura.name = "SMTR: Bilhetagem Integração - Captura (subflow)"
bilhetagem_integracao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_integracao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_integracao_captura = set_default_parameters(
    flow=bilhetagem_integracao_captura,
    default_parameters=constants.BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS.value
    | constants.BILHETAGEM_INTEGRACAO_CAPTURE_PARAMS.value,
)


# BILHETAGEM GPS - CAPTURA A CADA 5 MINUTOS #

bilhetagem_tracking_captura = deepcopy(default_capture_flow)
bilhetagem_tracking_captura.name = "SMTR: Bilhetagem GPS Validador - Captura"
bilhetagem_tracking_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_tracking_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_tracking_captura = set_default_parameters(
    flow=bilhetagem_tracking_captura,
    default_parameters=constants.BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS.value
    | constants.BILHETAGEM_TRACKING_CAPTURE_PARAMS.value,
)

bilhetagem_tracking_captura.state_handlers.append(skip_if_running_handler)


bilhetagem_tracking_captura.schedule = every_5_minutes

# BILHETAGEM RESSARCIMENTO - SUBFLOW PARA RODAR DIARIAMENTE #

bilhetagem_ressarcimento_captura = deepcopy(default_capture_flow)
bilhetagem_ressarcimento_captura.name = (
    "SMTR: Bilhetagem Ressarcimento - Captura (subflow)"
)
bilhetagem_ressarcimento_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_ressarcimento_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_ressarcimento_captura = set_default_parameters(
    flow=bilhetagem_ressarcimento_captura,
    default_parameters=constants.BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS.value,
)

# BILHETAGEM AUXILIAR - SUBFLOW PARA RODAR ANTES DE CADA MATERIALIZAÇÃO #

bilhetagem_auxiliar_captura = deepcopy(default_capture_flow)
bilhetagem_auxiliar_captura.name = "SMTR: Bilhetagem Auxiliar - Captura (subflow)"
bilhetagem_auxiliar_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_auxiliar_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_auxiliar_captura = set_default_parameters(
    flow=bilhetagem_auxiliar_captura,
    default_parameters=constants.BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS.value,
)

# MATERIALIZAÇÃO #

# Transação
bilhetagem_materializacao_transacao = deepcopy(default_materialization_flow)
bilhetagem_materializacao_transacao.name = (
    "SMTR: Bilhetagem Transação - Materialização (subflow)"
)
bilhetagem_materializacao_transacao.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_materializacao_transacao.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_materializacao_transacao_parameters = {
    "source_dataset_ids": [constants.BILHETAGEM_DATASET_ID.value],
    "source_table_ids": [
        constants.BILHETAGEM_TRANSACAO_CAPTURE_PARAMS.value["table_id"]
    ],
    "capture_intervals_minutes": [
        constants.BILHETAGEM_TRANSACAO_CAPTURE_PARAMS.value["interval_minutes"]
    ],
} | constants.BILHETAGEM_MATERIALIZACAO_TRANSACAO_PARAMS.value

bilhetagem_materializacao_transacao = set_default_parameters(
    flow=bilhetagem_materializacao_transacao,
    default_parameters=bilhetagem_materializacao_transacao_parameters,
)


bilhetagem_materializacao_transacao_riocard = deepcopy(default_materialization_flow)
bilhetagem_materializacao_transacao_riocard.name = (
    "SMTR: Bilhetagem Transação RioCard - Materialização"
)
bilhetagem_materializacao_transacao_riocard.storage = GCS(
    emd_constants.GCS_FLOWS_BUCKET.value
)
bilhetagem_materializacao_transacao_riocard.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_materializacao_transacao_riocard = set_default_parameters(
    flow=bilhetagem_materializacao_transacao_riocard,
    default_parameters=constants.BILHETAGEM_MATERIALIZACAO_TRANSACAO_RIOCARD_PARAMS.value,
)

# bilhetagem_materializacao_transacao_riocard.schedule = every_day_hour_five

# Ordem Pagamento

bilhetagem_materializacao_ordem_pagamento = deepcopy(default_materialization_flow)
bilhetagem_materializacao_ordem_pagamento.name = (
    "SMTR: Bilhetagem Ordem Pagamento - Materialização (subflow)"
)
bilhetagem_materializacao_ordem_pagamento.storage = GCS(
    emd_constants.GCS_FLOWS_BUCKET.value
)
bilhetagem_materializacao_ordem_pagamento.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)


ordem_pagamento_sources_table_ids = [
    constants.BILHETAGEM_TRANSACAO_CAPTURE_PARAMS.value["table_id"]
] + [d["table_id"] for d in constants.BILHETAGEM_ORDEM_PAGAMENTO_CAPTURE_PARAMS.value]

bilhetagem_materializacao_ordem_pagamento_parameters = {
    "source_dataset_ids": [
        constants.BILHETAGEM_DATASET_ID.value for _ in ordem_pagamento_sources_table_ids
    ],
    "source_table_ids": ordem_pagamento_sources_table_ids,
    "capture_intervals_minutes": [
        constants.BILHETAGEM_TRANSACAO_CAPTURE_PARAMS.value["interval_minutes"]
    ]
    + [
        d["interval_minutes"]
        for d in constants.BILHETAGEM_ORDEM_PAGAMENTO_CAPTURE_PARAMS.value
    ],
} | constants.BILHETAGEM_MATERIALIZACAO_ORDEM_PAGAMENTO_PARAMS.value

bilhetagem_materializacao_ordem_pagamento = set_default_parameters(
    flow=bilhetagem_materializacao_ordem_pagamento,
    default_parameters=bilhetagem_materializacao_ordem_pagamento_parameters,
)

# Integração

bilhetagem_materializacao_integracao = deepcopy(default_materialization_flow)
bilhetagem_materializacao_integracao.name = (
    "SMTR: Bilhetagem Integração - Materialização (subflow)"
)
bilhetagem_materializacao_integracao.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_materializacao_integracao.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_materializacao_integracao_parameters = {
    "source_dataset_ids": [constants.BILHETAGEM_DATASET_ID.value],
    "source_table_ids": [
        constants.BILHETAGEM_INTEGRACAO_CAPTURE_PARAMS.value["table_id"]
    ],
    "capture_intervals_minutes": [
        constants.BILHETAGEM_INTEGRACAO_CAPTURE_PARAMS.value["interval_minutes"]
    ],
} | constants.BILHETAGEM_MATERIALIZACAO_INTEGRACAO_PARAMS.value

bilhetagem_materializacao_integracao = set_default_parameters(
    flow=bilhetagem_materializacao_integracao,
    default_parameters=bilhetagem_materializacao_integracao_parameters,
)


# GPS Validador

bilhetagem_materializacao_gps_validador = deepcopy(default_materialization_flow)
bilhetagem_materializacao_gps_validador.name = (
    "SMTR: Bilhetagem GPS Validador - Materialização (subflow)"
)
bilhetagem_materializacao_gps_validador.storage = GCS(
    emd_constants.GCS_FLOWS_BUCKET.value
)
bilhetagem_materializacao_gps_validador.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_materializacao_gps_validador = set_default_parameters(
    flow=bilhetagem_materializacao_gps_validador,
    default_parameters=constants.BILHETAGEM_MATERIALIZACAO_GPS_VALIDADOR_GENERAL_PARAMS.value,
)

bilhetagem_materializacao_gps_validador.state_handlers.append(skip_if_running_handler)


# Validação dos dados

bilhetagem_validacao_jae = deepcopy(default_materialization_flow)
bilhetagem_validacao_jae.name = "SMTR: Bilhetagem Validação Jaé - Materialização"

bilhetagem_validacao_jae.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_validacao_jae = set_default_parameters(
    flow=bilhetagem_validacao_jae,
    default_parameters=constants.BILHETAGEM_MATERIALIZACAO_VALIDACAO_JAE_PARAMS.value,
)

# bilhetagem_validacao_jae.schedule = every_day_hour_seven


# RECAPTURA #

bilhetagem_recaptura = deepcopy(default_capture_flow)
bilhetagem_recaptura.name = "SMTR: Bilhetagem - Recaptura (subflow)"
bilhetagem_recaptura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_recaptura = set_default_parameters(
    flow=bilhetagem_recaptura,
    default_parameters=constants.BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS.value
    | {"recapture": True},
)


# TRATAMENTO - RODA DE HORA EM HORA, RECAPTURAS + CAPTURA AUXILIAR + MATERIALIZAÇÃO #

with Flow(
    "SMTR: Bilhetagem Transação - Tratamento",
    code_owners=["caio", "fernanda", "boris", "rodrigo", "rafaelpinheiro"],
) as bilhetagem_transacao_tratamento:
    # Configuração #

    capture = Parameter("capture", default=True)
    materialize = Parameter("materialize", default=True)

    timestamp = get_rounded_timestamp(
        interval_minutes=constants.BILHETAGEM_TRATAMENTO_INTERVAL.value
    )

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=bilhetagem_transacao_tratamento.name + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()

    with case(capture, True):
        # Recaptura Transação

        run_recaptura_transacao = create_flow_run(
            flow_name=bilhetagem_recaptura.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            parameters=constants.BILHETAGEM_TRANSACAO_CAPTURE_PARAMS.value,
        )

        wait_recaptura_transacao_true = wait_for_flow_run(
            run_recaptura_transacao,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        run_recaptura_transacao_riocard = create_flow_run(
            flow_name=bilhetagem_recaptura.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            parameters=constants.BILHETAGEM_TRANSACAO_RIOCARD_CAPTURE_PARAMS.value,
            upstream_tasks=[wait_recaptura_transacao_true],
        )

        wait_recaptura_transacao_riocard_true = wait_for_flow_run(
            run_recaptura_transacao_riocard,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        # Recaptura Fiscalização

        run_recaptura_fiscalizacao = create_flow_run(
            flow_name=bilhetagem_recaptura.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            parameters=constants.BILHETAGEM_FISCALIZACAO_CAPTURE_PARAMS.value,
        )

        wait_recaptura_fiscalizacao_true = wait_for_flow_run(
            run_recaptura_fiscalizacao,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        # Captura Auxiliar

        runs_captura = create_flow_run.map(
            flow_name=unmapped(bilhetagem_auxiliar_captura.name),
            project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            parameters=constants.BILHETAGEM_CAPTURE_PARAMS.value,
            labels=unmapped(LABELS),
        )

        runs_captura.set_upstream(wait_recaptura_transacao_true)

        wait_captura_true = wait_for_flow_run.map(
            runs_captura,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

        # Recaptura Auxiliar

        runs_recaptura_auxiliar = create_flow_run.map(
            flow_name=unmapped(bilhetagem_recaptura.name),
            project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            parameters=constants.BILHETAGEM_CAPTURE_PARAMS.value,
            labels=unmapped(LABELS),
        )

        runs_recaptura_auxiliar.set_upstream(wait_captura_true)

        wait_recaptura_auxiliar_true = wait_for_flow_run.map(
            runs_recaptura_auxiliar,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

    with case(capture, False):
        (
            wait_captura_false,
            wait_recaptura_auxiliar_false,
            wait_recaptura_transacao_false,
        ) = task(
            lambda: [None, None, None], name="assign_none_to_capture_runs", nout=3
        )()

    wait_captura = merge(wait_captura_false, wait_captura_true)
    wait_recaptura_auxiliar = merge(
        wait_recaptura_auxiliar_false, wait_recaptura_auxiliar_true
    )
    wait_recaptura_transacao = merge(
        wait_recaptura_transacao_false, wait_recaptura_transacao_true
    )

    with case(materialize, True):
        materialize_timestamp = get_current_timestamp(
            timestamp=timestamp, return_str=True
        )
        # Materialização
        run_materializacao_transacao = create_flow_run(
            flow_name=bilhetagem_materializacao_transacao.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            upstream_tasks=[
                wait_captura,
                wait_recaptura_auxiliar,
                wait_recaptura_transacao,
            ],
            parameters={
                "timestamp": materialize_timestamp,
            },
        )

        wait_materializacao_transacao = wait_for_flow_run(
            run_materializacao_transacao,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        run_materializacao_gps_validador = create_flow_run(
            flow_name=bilhetagem_materializacao_gps_validador.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            parameters={
                "table_id": constants.BILHETAGEM_MATERIALIZACAO_GPS_VALIDADOR_TABLE_ID.value,
                "timestamp": materialize_timestamp,
            },
            upstream_tasks=[wait_materializacao_transacao],
        )

        wait_materializacao_gps_validador = wait_for_flow_run(
            run_materializacao_gps_validador,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        run_materializacao_gps_validador_van = create_flow_run(
            flow_name=bilhetagem_materializacao_gps_validador.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            parameters={
                "table_id": constants.BILHETAGEM_MATERIALIZACAO_GPS_VALIDADOR_VAN_TABLE_ID.value,
                "timestamp": materialize_timestamp,
            },
            upstream_tasks=[wait_materializacao_gps_validador],
        )

        wait_materializacao_gps_validador_van = wait_for_flow_run(
            run_materializacao_gps_validador_van,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

bilhetagem_transacao_tratamento.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_transacao_tratamento.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# bilhetagem_transacao_tratamento.schedule = every_hour

# CAPTURA/TRATAMENTO - ORDEM PAGAMENTO:
# CAPTURA + RECAPTURA + MATERIALIZAÇÃO

with Flow(
    "SMTR: Bilhetagem Ordem Pagamento - Captura/Tratamento",
    code_owners=["caio", "fernanda", "boris", "rodrigo", "rafaelpinheiro"],
) as bilhetagem_ordem_pagamento_captura_tratamento:
    capture = Parameter("capture", default=True)
    materialize = Parameter("materialize", default=True)

    timestamp = get_rounded_timestamp(
        interval_minutes=constants.BILHETAGEM_TRATAMENTO_INTERVAL.value
    )

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=bilhetagem_ordem_pagamento_captura_tratamento.name + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()

    # Captura #
    with case(capture, True):
        runs_captura = create_flow_run.map(
            flow_name=unmapped(bilhetagem_ressarcimento_captura.name),
            project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            parameters=constants.BILHETAGEM_ORDEM_PAGAMENTO_CAPTURE_PARAMS.value,
            labels=unmapped(LABELS),
        )

        wait_captura = wait_for_flow_run.map(
            runs_captura,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

        runs_captura_integracao = create_flow_run(
            flow_name=unmapped(bilhetagem_integracao_captura.name),
            project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            labels=unmapped(LABELS),
            upstream_tasks=[wait_captura],
        )

        wait_captura_integracao = wait_for_flow_run(
            runs_captura_integracao,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

        # Recaptura #

        runs_recaptura = create_flow_run.map(
            flow_name=unmapped(bilhetagem_recaptura.name),
            project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            parameters=constants.BILHETAGEM_ORDEM_PAGAMENTO_CAPTURE_PARAMS.value,
            labels=unmapped(LABELS),
        )

        runs_recaptura.set_upstream(wait_captura)

        wait_recaptura_true = wait_for_flow_run.map(
            runs_recaptura,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

        # Recaptura Integração

        run_recaptura_integracao = create_flow_run(
            flow_name=bilhetagem_recaptura.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            parameters=constants.BILHETAGEM_INTEGRACAO_CAPTURE_PARAMS.value,
            upstream_tasks=[wait_recaptura_true, wait_captura_integracao],
        )

        wait_recaptura_integracao_true = wait_for_flow_run(
            run_recaptura_integracao,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

    with case(capture, False):
        wait_recaptura_false, wait_recaptura_integracao_false = task(
            lambda: [None, None], name="assign_none_to_recapture", nout=2
        )()

    wait_recaptura = merge(wait_recaptura_true, wait_recaptura_false)
    wait_recaptura_integracao = merge(
        wait_recaptura_integracao_true, wait_recaptura_integracao_false
    )

    # Materialização #

    with case(materialize, True):
        materialize_timestamp = get_current_timestamp(
            timestamp=timestamp,
            return_str=True,
        )

        run_materializacao = create_flow_run(
            flow_name=bilhetagem_materializacao_ordem_pagamento.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            upstream_tasks=[wait_recaptura, wait_recaptura_integracao],
            parameters={
                "timestamp": materialize_timestamp,
            },
        )

        wait_materializacao = wait_for_flow_run(
            run_materializacao,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        run_materializacao_integracao = create_flow_run(
            flow_name=bilhetagem_materializacao_integracao.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            upstream_tasks=[
                wait_materializacao,
            ],
            parameters={
                "timestamp": materialize_timestamp,
            },
        )

        wait_materializacao_integracao = wait_for_flow_run(
            run_materializacao_integracao,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

    bilhetagem_ordem_pagamento_captura_tratamento.set_reference_tasks(
        [wait_materializacao_integracao, wait_recaptura]
    )

bilhetagem_ordem_pagamento_captura_tratamento.storage = GCS(
    emd_constants.GCS_FLOWS_BUCKET.value
)
bilhetagem_ordem_pagamento_captura_tratamento.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)


# bilhetagem_ordem_pagamento_captura_tratamento.schedule = every_day_hour_five
