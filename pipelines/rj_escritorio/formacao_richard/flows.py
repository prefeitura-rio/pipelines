# -*- coding: utf-8 -*-
from copy import deepcopy

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters
from pipelines.rj_escritorio.formacao_richard.schedules import gsheets_schedule

rj_escritorio_formacao_richard_gsheets_flow = deepcopy(dump_url_flow)
rj_escritorio_formacao_richard_gsheets_flow.name = "EMD: formação - Richard - Aula 3"
rj_escritorio_formacao_richard_gsheets_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
rj_escritorio_formacao_richard_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)

rj_escritorio_formacao_richard_gsheets_default_parameters = {
    "dataset_id": "teste_formacao_richard"
}
rj_escritorio_formacao_richard_gsheets_flow = set_default_parameters(
    rj_escritorio_formacao_richard_gsheets_flow,
    default_parameters=rj_escritorio_formacao_richard_gsheets_default_parameters,
)
rj_escritorio_formacao_richard_gsheets_flow.schedule = gsheets_schedule
