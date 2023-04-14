# -*- coding: utf-8 -*-
"""
Fluxo para ingestao de Google Sheet para o BigQuery
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

# from pipelines.rj_sme.dump_url_educacao_basica.schedules import (
#     gsheets_year_update_schedule,

formacao_dump_gsheets_flow = deepcopy(dump_url_flow)
formacao_dump_gsheets_flow.name = "EMD: Formação Edisom - Ingerir CSV do Google Drive"
formacao_dump_gsheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
formacao_dump_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
)

formacao_dump_gsheets_default_parameters = {
    "dataset_id": "formacao_edison",
}
formacao_dump_gsheets_flow = set_default_parameters(
    formacao_dump_gsheets_flow,
    default_parameters=formacao_dump_gsheets_default_parameters,
)

formacao_dump_gsheets_flow.schedule = None
