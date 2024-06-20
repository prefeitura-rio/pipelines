# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_recurso
"""
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect import Parameter, case, task
from prefect.tasks.control_flow import merge
from prefect.utilities.edges import unmapped

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
from pipelines.rj_smtr.tasks import get_current_timestamp

from pipelines.rj_smtr.flows import default_capture_flow, default_materialization_flow

# from pipelines.rj_smtr.schedules import every_day


# CAPTURA DOS TICKETS #

sppo_recurso_captura = deepcopy(default_capture_flow)
sppo_recurso_captura.name = "SMTR: Subsídio Recursos - Captura (subflow)"
sppo_recurso_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_recurso_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
sppo_recurso_captura = set_default_parameters(
    flow=sppo_recurso_captura,
    default_parameters=constants.SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS.value,
)
# RECAPTURA DOS TICKETS #
sppo_recurso_recaptura = deepcopy(default_capture_flow)
sppo_recurso_recaptura.name = "SMTR: Subsídio Recursos - Recaptura (subflow)"
sppo_recurso_recaptura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_recurso_recaptura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
sppo_recurso_recaptura = set_default_parameters(
    flow=sppo_recurso_recaptura,
    default_parameters=constants.SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS.value
    | {"recapture": True},
)

# MATERIALIZAÇÃO DOS TICKETS #

sppo_recurso_materializacao = deepcopy(default_materialization_flow)
sppo_recurso_materializacao.name = "SMTR: Subsídio Recursos - Materialização (subflow)"
sppo_recurso_materializacao.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_recurso_materializacao.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

sppo_recurso_materializacao = set_default_parameters(
    flow=sppo_recurso_materializacao,
    default_parameters=constants.SUBSIDIO_SPPO_RECURSOS_MATERIALIZACAO_PARAMS.value,
)

with Flow(
    "SMTR: Subsídio Recursos - Captura/Tratamento",
    code_owners=["carolinagomes", "rafaelpinheiro"],
) as subsidio_sppo_recurso:
    capture = Parameter("capture", default=True)
    materialize = Parameter("materialize", default=True)
    recapture = Parameter("recapture", default=True)
    data_recurso = Parameter("data_recurso", default=None)
    table_id = Parameter("table_id", default=None)
    interval_minutes = Parameter("interval_minutes", default=1440)
    timestamp = get_current_timestamp(data_recurso, return_str=True)
    exclude = Parameter("exclude", default=None)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=subsidio_sppo_recurso.name + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()

    recursos_capture_parameters = [
        {
            "table_id": v,
            "extract_params": {
                "data_recurso": timestamp,
                **constants.SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS.value[
                    "extract_params"
                ],
            },
        }
        for v in constants.SUBSIDIO_SPPO_RECURSO_TABLE_CAPTURE_PARAMS.value
    ]

    table_params = task(
        lambda tables, exclude: (
            [t for t in tables if t["table_id"] not in exclude]
            if exclude is not None
            else tables
        ),
        checkpoint=False,
        name="get_tables_to_run",
    )(tables=constants.SUBSIDIO_SPPO_RECURSOS_TABLE_IDS.value, exclude=exclude)

    # Captura dos dados #
    with case(capture, True):
        run_captura = create_flow_run.map(
            flow_name=unmapped(sppo_recurso_captura.name),
            project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            parameters=recursos_capture_parameters,
            labels=unmapped(LABELS),
        )

        wait_captura_true = wait_for_flow_run.map(
            run_captura,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

    with case(capture, False):
        wait_captura_false = task(
            lambda: [None],
            checkpoint=False,
            name="assign_none_to_previous_runs",
        )()

    wait_captura = merge(wait_captura_true, wait_captura_false)

    # Recaptura dos dados #

    with case(recapture, True):
        run_recaptura = create_flow_run.map(
            flow_name=unmapped(sppo_recurso_recaptura.name),
            project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            parameters=recursos_capture_parameters,
            labels=unmapped(LABELS),
        )

        wait_recaptura_true = wait_for_flow_run.map(
            run_recaptura,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

    with case(recapture, False):
        wait_recaptura_false = task(
            lambda: [None], checkpoint=False, name="assign_none_to_previous_runs"
        )()

    wait_recaptura = merge(wait_recaptura_true, wait_recaptura_false)

    # Materialização dos dados #

    with case(materialize, True):
        run_materializacao = create_flow_run.map(
            flow_name=unmapped(sppo_recurso_materializacao.name),
            project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
            labels=unmapped(LABELS),
            parameters=table_params,
            upstream_tasks=[wait_captura],
        )

        wait_materializacao_true = wait_for_flow_run.map(
            run_materializacao,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

    with case(materialize, False):
        wait_materializacao_false = task(
            lambda: [None], checkpoint=False, name="assign_none_to_previous_runs"
        )()
    wait_materializacao = merge(wait_materializacao_true, wait_materializacao_false)

    subsidio_sppo_recurso.set_reference_tasks(
        [wait_materializacao, wait_recaptura, wait_captura]
    )

subsidio_sppo_recurso.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_recurso.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

# Schedule
# subsidio_sppo_recurso.schedule = every_day
