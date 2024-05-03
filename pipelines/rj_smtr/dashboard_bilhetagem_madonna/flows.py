# -*- coding: utf-8 -*-
"""
Flows for dashboard_bilhetagem_madonna
"""


from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.utils.decorators import Flow

from pipelines.utils.tasks import (
    get_current_flow_labels,
    get_current_flow_mode,
)

from pipelines.utils.utils import set_default_parameters


from pipelines.rj_smtr.br_rj_riodejaneiro_bilhetagem.flows import bilhetagem_recaptura
from pipelines.rj_smtr.flows import (
    default_materialization_flow,
)

from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.constants import constants

from pipelines.rj_smtr.schedules import every_10_minutes_dev

from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

bilhetagem_materializacao_madonna = deepcopy(default_materialization_flow)
bilhetagem_materializacao_madonna.name = (
    "SMTR: Bilhetagem Madonna - Materialização (subflow)"
)
bilhetagem_materializacao_madonna.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_materializacao_madonna.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    # labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

bilhetagem_materializacao_madonna_parameters = {
    "dataset_id": "dashboard_bilhetagem_madonna",
    "table_id": "transacao_gentileza",
    # "table_id": "transacao_madonna",
    "upstream": True,
}

bilhetagem_materializacao_madonna = set_default_parameters(
    flow=bilhetagem_materializacao_madonna,
    default_parameters=bilhetagem_materializacao_madonna_parameters,
)

bilhetagem_materializacao_madonna.schedule = every_10_minutes

with Flow("SMTR: Bilhetagem Madonna - Tratamento") as bilhetagem_madonna:
    LABELS = get_current_flow_labels()

    ## RECAPTURA ##

    # run_recaptura_transacao = create_flow_run(
    #     flow_name=bilhetagem_recaptura.name,
    #     # project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
    #     project_name="staging",
    #     labels=emd_constants.RJ_SMTR_AGENT_LABEL.value,
    #     parameters=constants.BILHETAGEM_TRANSACAO_CAPTURE_PARAMS.value,
    # )

    # wait_recaptura_transacao = wait_for_flow_run(
    #     run_recaptura_transacao,
    #     stream_states=True,
    #     stream_logs=True,
    #     raise_final_state=True,
    # )

    ## MATERIALIZAÇÃO ##

    # run_materializacao_madonna = create_flow_run(
    #     flow_name=bilhetagem_materializacao_madonna.name,
    #     # project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
    #     project_name="staging",
    #     labels=LABELS,
    #     # upstream_tasks=[
    #     #     wait_recaptura_transacao,
    #     # ],
    # )

    # wait_materializacao_madonna = wait_for_flow_run(
    #     run_materializacao_madonna,
    #     stream_states=True,
    #     stream_logs=True,
    #     raise_final_state=True,
    # )

    MODE = get_current_flow_mode(LABELS)
    dbt_client = get_k8s_dbt_client(mode=MODE)
    RUNS = run_dbt_model(
        dbt_client=dbt_client,
        dataset_id="dashboard_bilhetagem_madonna",
        table_id="transacao_gentileza",
        upstream=True,
    )

bilhetagem_madonna.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_madonna.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    # labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

bilhetagem_madonna.schedule = every_10_minutes_dev
