# -*- coding: utf-8 -*-
"""
Database dumping flows for segovi project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_escritorio.dump_url_formacao.schedules import (
    gsheets_one_minute_update_schedule,
)
from pipelines.utils.dump_url_formacao.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

formacao_gsheets_flow = deepcopy(dump_url_flow)
formacao_gsheets_flow.name = "EMD: Formação GSheets - Ingerir tabelas de URL"
formacao_gsheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
formacao_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
)

formacao_gsheets_flow_parameters = {
    "dataset_id": "test_formacao",
    "dump_mode": "overwrite",
    "url": "https://docs.google.com/spreadsheets/d/1uF-Gt5AyZmxCQQEaebvWF4ddRHeVuL6ANuoaY_-uAXE\
        /edit#gid=0",
    "url_type": "google_sheet",
    "gsheets_sheet_name": "sheet_1",
    "table_id": "test_table",
}
formacao_gsheets_flow = set_default_parameters(
    formacao_gsheets_flow, default_parameters=formacao_gsheets_flow_parameters
)

formacao_gsheets_flow.schedule = gsheets_one_minute_update_schedule
