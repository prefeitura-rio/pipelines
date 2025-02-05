# -*- coding: utf-8 -*-
"""
Database dumping flows
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_smfp.dump_inadimplente.schedules import (
#     inadimplente_weekly_update_schedule,
# )
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

smfp_inadimplente_flow = deepcopy(dump_sql_flow)
smfp_inadimplente_flow.name = "SMFP: Inadimplentes - Ingerir tabelas de banco SQL"
smfp_inadimplente_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
smfp_inadimplente_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
)

inadimplente_default_parameters = {
    "db_database": "DBINAD",
    "db_host": "10.3.23.158",
    "db_port": "1433",
    "db_type": "sql_server",
    "vault_secret_path": "formacao-iptu-inadimplentes",
    "dataset_id": "iptu_inadimplentes",
}
smfp_inadimplente_flow = set_default_parameters(
    smfp_inadimplente_flow, default_parameters=inadimplente_default_parameters
)

# smfp_inadimplente_flow.schedule = inadimplente_weekly_update_schedule
