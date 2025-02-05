# -*- coding: utf-8 -*-
"""
Database dumping flows for SMFP SIGMA system.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# importa o schedule
# from pipelines.rj_smfp.dump_db_sigma.schedules import (
#     sigma_daily_update_schedule,
# )
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

rj_smfp_dump_db_sigma_flow = deepcopy(dump_sql_flow)
rj_smfp_dump_db_sigma_flow.name = (
    "SMFP: SIGMA - Sancao Fornecedor - Ingerir tabelas de banco SQL"
)
rj_smfp_dump_db_sigma_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)

rj_smfp_dump_db_sigma_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,  # label do agente
    ],
)

rj_smfp_dump_db_sigma_default_parameters = {
    "db_database": "CP01.SMF",
    "db_host": "10.90.31.22",
    "db_port": "1521",
    "db_type": "oracle",
    "dataset_id": "adm_orcamento_sigma",
    "vault_secret_path": "db-sigma",
}

rj_smfp_dump_db_sigma_flow = set_default_parameters(
    rj_smfp_dump_db_sigma_flow,
    default_parameters=rj_smfp_dump_db_sigma_default_parameters,
)

# rj_smfp_dump_db_sigma_flow.schedule = sigma_daily_update_schedule
