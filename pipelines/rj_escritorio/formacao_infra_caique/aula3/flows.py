# -*- coding: utf-8 -*-
"""
Cópia do flow de template para dump de URL
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio_formacao_infra_caique.aula3.schedules import (
    gsheets_schedule,
)
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

formacao_dump_gsheets_flow = deepcopy(dump_url_flow)
formacao_dump_gsheets_flow.name = "Formação exemplo: Ingerir CSV do Google Drive"
formacao_dump_gsheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
formacao_dump_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
)

formacao_dump_gsheets_default_parameters = {
    "dataset_id": "teste_formacao",
}

rj_escritorio_formacao_infra_caique_aula3_flow = set_default_parameters(
    formacao_dump_gsheets_flow,
    default_parameters=formacao_dump_gsheets_default_parameters,
)

rj_escritorio_formacao_infra_caique_aula3_flow.schedule = gsheets_schedule
