# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_recurso
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
from pipelines.utils.utils import set_default_parameters
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

from pipelines.rj_smtr.flows import (
    default_capture_flow,
)  # -> TODO: alterar função de captura de dados brutos

# SETUP #

sppo_recurso_captura = deepcopy(default_capture_flow)
sppo_recurso_captura.name = "Subsídio SPPO - Recursos Captura"
sppo_recurso_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_recurso_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
sppo_recurso_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_recurso_captura = set_default_parameters(
    flow=sppo_recurso_captura,
    default_parameters=constants.SUBSIDIO_SPPO_RECURSO_DEFAULT_PARAM.value,
)

with Flow(
    "SMTR: Subsídio Recursos Viagens Individuais - Captura",
    code_owners=["carolinagomes", "igorlaltuf"],
) as subsidio_sppo_recurso:
    capture = Parameter("capture", default=True)
    # timestamp = get_current_timestamp()
    timestamp = 34459200

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=subsidio_sppo_recurso.name + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()

    with case(capture, True):
        run_captura = create_flow_run.map(
            flow_name=sppo_recurso_captura.name,
            project_name="staging",
            parameters=sppo_recurso_captura,
            labels=LABELS,
        )

        wait_captura_true = wait_for_flow_run.map(
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

subsidio_sppo_recurso.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_recurso.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
