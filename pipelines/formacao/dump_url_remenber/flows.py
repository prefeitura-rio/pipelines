# -*- coding: utf-8 -*-
"""
Database dumping flows for dump_url_remenber
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.formacao.dump_url_remenber.schedules import (
    gsheets_daily_update_schedule,
)

from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

formacao_dump_url_remenber_flow = deepcopy(dump_url_flow)
formacao_dump_url_remenber_flow.name = (
    "Dump url remember"
)

formacao_dump_url_remenber_gsheets_flow.storage = GCS(constants.GCS_BUCKET.value)
formacao_dump_url_remenber_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SEGOVI_AGENT_LABEL.value
            ],
)

formacao_dump_url_remenber_default_parameters = {
    "dump_mode": "overwrite",
    "url_type": "google_drive",
    "dataset_id": "db_remember_teste",
}

formacao_dump_url_remenber_flow = set_default_parameters(
    formacao_dump_url_remenber_flow,
    default_parameters=formacao_dump_url_remenber_default_parameters,
)

formacao_dump_url_remenber_gsheets_flow.schedule = gsheets_daily_update_schedule
