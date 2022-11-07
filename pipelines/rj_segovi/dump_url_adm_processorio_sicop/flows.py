# -*- coding: utf-8 -*-
"""
Database dumping flows for segovi processorio sicop
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_segovi.dump_url_adm_processorio_sicop.schedules import (
    gsheets_daily_update_schedule,
)
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

segovi_processorio_sicop_gsheets_flow = deepcopy(dump_url_flow)
segovi_processorio_sicop_gsheets_flow.name = (
    "SEGOVI: Processo.rio-SICOP - Ingerir tabelas do Google Drive"
)
segovi_processorio_sicop_gsheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
segovi_processorio_sicop_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
)

segovi_processorio_sicop_default_parameters = {
    "dump_mode": "overwrite",
    "url_type": "google_drive",
    "dataset_id": "adm_processorio_sicop",
}
segovi_processorio_sicop_flow = set_default_parameters(
    segovi_processorio_sicop_gsheets_flow,
    default_parameters=segovi_processorio_sicop_default_parameters,
)

segovi_processorio_sicop_flow.schedule = gsheets_daily_update_schedule
