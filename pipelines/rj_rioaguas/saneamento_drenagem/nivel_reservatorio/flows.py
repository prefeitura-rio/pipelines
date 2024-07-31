# -*- coding: utf-8 -*-
# pylint: disable= line-too-long

"""
Database dumping flows for nivel_reservatorio project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_rioaguas.saneamento_drenagem.nivel_reservatorio.schedules import (
    update_schedule_nivel_reservatorio,
)

from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

nivel_gsheets_flow = deepcopy(dump_url_flow)
nivel_gsheets_flow.name = "RIOAGUAS: Drenagem - Nivel dos reservatorios"
nivel_gsheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
nivel_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_RIOAGUAS_AGENT_LABEL.value,
    ],
)

nivel_gsheets_flow_parameters = {
    "dataset_id": "saneamento_drenagem",
    "dump_mode": "overwrite",
    "url": "https://docs.google.com/spreadsheets/d/1zM0N_PonkALEK3YD2A4DF9W10Cm2n99_IiySm8zygqk/edit#gid=1343658906",  # noqa
    "url_type": "google_sheet",
    "gsheets_sheet_name": "Reservatórios",
    "table_id": "nivel_reservatorio",
}

nivel_gsheets_flow = set_default_parameters(
    nivel_gsheets_flow, default_parameters=nivel_gsheets_flow_parameters
)

# nivel_gsheets_flow.schedule = update_schedule_nivel_reservatorio
nivel_gsheets_flow.schedule = None

