# -*- coding: utf-8 -*-
"""
Database dumping flows for Setur project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_escritorio.dump_url_turismo.schedules import (
    gsheets_daily_update_schedule,
)
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

setur_gsheets_flow = deepcopy(dump_url_flow)
setur_gsheets_flow.name = "SETUR: Google Sheets - Ingerir tabelas de URL"
setur_gsheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
setur_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
)

setur_gsheets_default_parameters = {}
setur_gsheets_flow = set_default_parameters(
    setur_gsheets_flow, default_parameters=setur_gsheets_default_parameters
)

setur_gsheets_flow.schedule = gsheets_daily_update_schedule
