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
from pipelines.rj_smtr.schedules import every_hour


# CAPTURA #

sppo_recurso_captura = deepcopy(default_capture_flow)
sppo_recurso_captura.name = "SMTR: Subsídio SPPO Recursos - Captura (subflow)"
sppo_recurso_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_recurso_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
sppo_recurso_captura = set_default_parameters(
    flow=sppo_recurso_captura,
    default_parameters=constants.SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS.value,
)

# MATERIALIZAÇÃO #

sppo_recurso_materializacao = deepcopy(default_materialization_flow)
sppo_recurso_materializacao.name = (
    "SMTR: Subsídio SPPO Recursos - Materialização (subflow)"
)
sppo_recurso_materializacao.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_recurso_materializacao.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

# a definir
sppo_recurso_materializacao_parameters = {}

sppo_recurso_materializacao = set_default_parameters(
    flow=sppo_recurso_materializacao,
    default_parameters=sppo_recurso_materializacao_parameters,
)

with Flow(
    "SMTR: Subsídio Recursos Viagens Individuais - Captura",
    code_owners=["carolinagomes", "igorlaltuf"],
) as subsidio_sppo_recurso:
    capture = Parameter("capture", default=True)
    materialize = Parameter("materialize", default=True)
    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=subsidio_sppo_recurso.name + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()

    with case(capture, True):
        run_captura = create_flow_run(
            flow_name=sppo_recurso_captura.name,
            project_name="staging",
            parameters=constants.SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS.value,
            labels=LABELS,
        )

        wait_captura_true = wait_for_flow_run(
            run_captura,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

    with case(capture, False):
        wait_captura_false = task(
            lambda: [None], checkpoint=False, name="assign_none_to_previous_runs"
        )()

    wait_captura = merge(wait_captura_true, wait_captura_false)

    with case(materialize, True):
        sppo_recurso_materializacao_parameters = {}

        run_materializacao = create_flow_run(
            flow_name=sppo_recurso_materializacao.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            parameters=sppo_recurso_materializacao_parameters,
            labels=LABELS,
            upstream_tasks=[wait_captura],
        )

        wait_materializacao = wait_for_flow_run(
            run_materializacao,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

subsidio_sppo_recurso.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_recurso.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

# subsidio_sppo_recurso.schedule = every_hour
