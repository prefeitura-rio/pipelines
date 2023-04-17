# -*- coding: utf-8 -*-
"""
Pipeline para ingestao de GoogleSheet no Datalake

"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.formacao_edison_aula3.schedules import gsheets_schedule
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

formacao_dump_gsheets_flow = deepcopy(dump_url_flow)
formacao_dump_gsheets_flow.name = "EMD: Formação Edison - Ingerir Google Sheet"
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

formacao_dump_gsheets_flow.schedule = gsheets_schedule
