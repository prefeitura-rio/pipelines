# -*- coding: utf-8 -*-
"""
Database dumping flows for formation project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_sms.dump_sheets.schedules import gsheets_one_minute_update_schedule
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

medicamentos_flow = deepcopy(dump_url_flow)
medicamentos_flow.name = (
    "SMS: Dump Medicamentos - Ingerir tabela mestra de medicamentos"
)
medicamentos_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
medicamentos_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

medicamentos_flow_parameters = {
    "url": "https://docs.google.com/spreadsheets/d/15dQSia-oL-c5nP-wKQOjlAQl7MVHiDFNv6W0RcYgcSY/edit?usp=sharing",
    "url_type": "google_sheet",
    "gsheets_sheet_name": "dados_mestres",
    "table_id": "medicamentos2",
    "dataset_id": "dados_mestres",
    "dump_mode": "append",
}

medicamentos_flow = set_default_parameters(
    medicamentos_flow, default_parameters=medicamentos_flow_parameters
)

# medicamentos_flow.schedule = gsheets_one_minute_update_schedule
