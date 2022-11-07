# -*- coding: utf-8 -*-
"""
Database dumping flows for segovi project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_escritorio.exemplo_dump_url.schedules import gsheets_daily_update_schedule
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

exemplo_dump_url_flow = deepcopy(dump_url_flow)
exemplo_dump_url_flow.name = "EMD: Exemplo Dump Url Google Sheets - Ingerir tabelas de URL"
exemplo_dump_url_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
exemplo_dump_url_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
)

exemplo_dump_url_default_parameters = {}
exemplo_dump_url_flow = set_default_parameters(
    exemplo_dump_url_flow, default_parameters=exemplo_dump_url_default_parameters
)

exemplo_dump_url_flow.schedule = gsheets_daily_update_schedule
