# -*- coding: utf-8 -*-
"""
Database  dumping flows for SEOP project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_sme.dump_url_educacao_basica.schedules import (
    gsheets_year_update_schedule,
)
from pipelines.utils.dump_url.flows import dump_url_flow
from pipelines.utils.utils import set_default_parameters

sme_gsheets_flow_prova_rio_2009_a_2016 = deepcopy(dump_url_flow)
sme_gsheets_flow_prova_rio_2009_a_2016.name = (
    "SME: Prova Rio 2009 a 2016 - Ingerir CSV do Google Drive"
)
sme_gsheets_flow_prova_rio_2009_a_2016.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sme_gsheets_flow_prova_rio_2009_a_2016.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SME_AGENT_LABEL.value,
    ],
)

sme_gsheets_default_parameters = {
    "dataset_id": "prova_rio_2009_a_2016_anonimizado",
}
sme_gsheets_flow_prova_rio_2009_a_2016 = set_default_parameters(
    sme_gsheets_flow_prova_rio_2009_a_2016,
    default_parameters=sme_gsheets_default_parameters,
)

sme_gsheets_flow_prova_rio_2009_a_2016.schedule = gsheets_year_update_schedule
