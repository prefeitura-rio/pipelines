# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_diretorios
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect import Parameter, task

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
    default_materialization_flow,
)
from pipelines.rj_smtr.tasks import get_rounded_timestamp, get_current_timestamp
from pipelines.rj_smtr.constants import constants

diretorios_materializacao_subflow = deepcopy(default_materialization_flow)
diretorios_materializacao_subflow.name = "SMTR: Diretórios - Materialização (subflow)"

diretorios_materializacao_subflow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
diretorios_materializacao_subflow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

diretorios_materializacao_subflow = set_default_parameters(
    flow=diretorios_materializacao_subflow,
    default_parameters=constants.DIRETORIO_MATERIALIZACAO_PARAMS.value,
)

with Flow(
    "SMTR: Diretórios - Materialização",
    code_owners=["caio", "fernanda", "boris", "rodrigo", "rafaelpinheiro"],
) as diretorios_materializacao:
    # Configuração #

    exclude = Parameter("exclude", default=None)

    timestamp = get_rounded_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=diretorios_materializacao.name + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()

    table_params = task(
        lambda tables, exclude: [t for t in tables if t["table_id"] not in exclude]
        if exclude is not None
        else tables,
        checkpoint=False,
        name="get_tables_to_run",
    )(tables=constants.DIRETORIO_MATERIALIZACAO_TABLE_PARAMS.value, exclude=exclude)

    run_materializacao = create_flow_run.map(
        flow_name=unmapped(diretorios_materializacao_subflow.name),
        project_name=unmapped("staging"),
        # project_name=unmapped(emd_constants.PREFECT_DEFAULT_PROJECT.value),
        labels=unmapped(LABELS),
        parameters=table_params,
    )

    wait_materializacao = wait_for_flow_run.map(
        run_materializacao,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )


diretorios_materializacao.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
diretorios_materializacao.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
