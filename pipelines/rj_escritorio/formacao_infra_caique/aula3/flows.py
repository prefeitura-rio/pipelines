# -*- coding: utf-8 -*-
"""
Cópia do flow de template para dump de URL
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

from pipelines.rj_escritorio.formacao_infra_caique.aula3.schedules import (
    gsheets_schedule,
)

caique_dump_gsheets_flow = deepcopy(dump_url_flow)
caique_dump_gsheets_flow.name = (
    "EMD: Formação_Caique | Ingerir planilha do Google Drive"
)
caique_dump_gsheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
caique_dump_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
)

caique_dump_gsheets_default_parameters = {
    "dataset_id": "formacao_caique",
}

caique_dump_gsheets_flow = set_default_parameters(
    caique_dump_gsheets_flow,
    default_parameters=caique_dump_gsheets_default_parameters,
)

caique_dump_gsheets_flow.schedule = gsheets_schedule
