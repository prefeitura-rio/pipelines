# -*- coding: utf-8 -*-
"""
Database dumping flows for dump_url_remenber
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_segovi.dump_url_dbdocumentos.schedules import (
    gsheets_daily_update_schedule,
)

from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

rj_segovi_dump_url_dbdocumentos_gsheets_flow = deepcopy(dump_url_flow)
rj_segovi_dump_url_dbdocumentos_gsheets_flow.name = "Dump URL db_documentos"

rj_segovi_dump_url_dbdocumentos_gsheets_flow.storage = GCS(
    constants.GCS_BUCKET.value,
)

rj_segovi_dump_url_dbdocumentos_gsheets_flow.run_config = KubernetesRun(
    image=constants.DUMP_URL_IMAGE.value,
    labels=[constants.RJ_SEGOVI_AGENT_LABEL.value],
)

rj_segovi_dump_url_dbdocumentos_default_parameters = {
    "dataset_id": "db_documentos",
    "dump_mode": "overwrite",
    "url_type": "google_drive",
}

rj_segovi_dump_url_dbdocumentos_gsheets_flow = set_default_parameters(
    rj_segovi_dump_url_dbdocumentos_gsheets_flow,
    default_parameters=rj_segovi_dump_url_dbdocumentos_default_parameters,
)

rj_segovi_dump_url_dbdocumentos_gsheets_flow.schedule = gsheets_daily_update_schedule
