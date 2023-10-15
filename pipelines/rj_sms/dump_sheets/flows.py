# -*- coding: utf-8 -*-
"""
Database dumping flows for sheets dump.
"""

from copy import deepcopy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters
from pipelines.rj_sms.dump_sheets.constants import constants as sheets_constants
from pipelines.rj_sms.dump_sheets.schedules import every_sunday_at_six_am


unidade_saude_flow = deepcopy(dump_url_flow)
unidade_saude_flow.name = "SMS: Dump Unidades de Saude - Ingerir tabela auxiliar"
unidade_saude_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
unidade_saude_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

unidade_saude_flow_parameters = {
    "url": "https://docs.google.com/spreadsheets/d/1EkYfxuN2bWD_q4OhHL8hJvbmQKmQKFrk0KLf6D7nKS4/edit?usp=sharing",  # noqa: E501
    "url_type": "google_sheet",
    "gsheets_sheet_name": "Sheet1",
    "table_id": "estabelecimento_auxiliar",
    "dataset_id": sheets_constants.DATASET_ID.value,
    "dump_mode": "overwrite",
}

unidade_saude_flow = set_default_parameters(
    unidade_saude_flow, default_parameters=unidade_saude_flow_parameters
)
unidade_saude_flow.schedule = every_sunday_at_six_am
