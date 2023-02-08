# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
"""
Database dumping flows for dump_url_remenber
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.formacao_dbdocumentos.remenber_csv.schedules import (
    gsheets_daily_update_schedule,
)

from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

formacao_dbdocumentos_remenber_csv_gsheets_flow = deepcopy(dump_url_flow)
formacao_dbdocumentos_remenber_csv_gsheets_flow.name = "Dump url remember"

formacao_dbdocumentos_remenber_csv_gsheets_flow.storage = GCS(
    constants.GCS_BUCKET.value
)
formacao_dbdocumentos_remenber_csv_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SEGOVI_AGENT_LABEL.value],
)

formacao_dbdocumentos_remenber_default_parameters = {
    "dump_mode": "overwrite",
    "url_type": "google_drive",
    "dataset_id": "db_remember_teste",
}

formacao_dbdocumentos_remenber_csv_flow = set_default_parameters(
    formacao_dbdocumentos_remenber_csv_gsheets_flow,
    default_parameters=formacao_dbdocumentos_remenber_default_parameters,
)

formacao_dbdocumentos_remenber_csv_flow.schedule = gsheets_daily_update_schedule
