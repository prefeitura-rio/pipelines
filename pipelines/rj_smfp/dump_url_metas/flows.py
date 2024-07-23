# -*- coding: utf-8 -*-
"""
Database dumping flows for segovi project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_smfp.dump_url_metas.schedules import gsheets_daily_update_schedule
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

smfp_gsheets_flow = deepcopy(dump_url_flow)
smfp_gsheets_flow.name = "SMFP: Google Sheets - Ingerir tabelas de URL"
smfp_gsheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
smfp_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
)

smfp_gsheets_default_parameters = {
    "materialize_to_datario": False,
}
smfp_gsheets_flow = set_default_parameters(
    smfp_gsheets_flow, default_parameters=smfp_gsheets_default_parameters
)

# smfp_gsheets_flow.schedule = gsheets_daily_update_schedule
