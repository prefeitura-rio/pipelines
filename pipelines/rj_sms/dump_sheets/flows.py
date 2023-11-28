# -*- coding: utf-8 -*-
"""
Database dumping flows for sheets dump.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.rj_sms.dump_sheets.schedules import sms_sheets_daily_update_schedule

# TODO: add code owner

dump_sms_sheets_flow = deepcopy(dump_url_flow)
dump_sms_sheets_flow.name = (
    "SMS: Dump Google Sheets - Ingerir planilhas do Google Sheets"
)
dump_sms_sheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)

dump_sms_sheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)

dump_sms_sheets_flow.schedule = sms_sheets_daily_update_schedule