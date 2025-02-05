# -*- coding: utf-8 -*-
"""
Database  dumping flows for SEOP project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_seop.dump_url_conservacao_ambiental.schedules import (
#     gsheets_year_update_schedule,
# )
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

seop_gsheets_flow = deepcopy(dump_url_flow)
seop_gsheets_flow.name = "SEOP: Concervacao Ambiental - Ingerir CSV do Google Drive"
seop_gsheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
seop_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SEOP_AGENT_LABEL.value,
    ],
)

seop_gsheets_default_parameters = {
    "dataset_id": "conservacao_ambiental_monitor_verde",
}
seop_gsheets_flow = set_default_parameters(
    seop_gsheets_flow, default_parameters=seop_gsheets_default_parameters
)

# seop_gsheets_flow.schedule = gsheets_year_update_schedule
