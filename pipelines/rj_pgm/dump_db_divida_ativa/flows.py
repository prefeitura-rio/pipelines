# -*- coding: utf-8 -*-
"""
Database dumping flows for DAM divida_ativa PGM
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# importa o schedule
# from pipelines.rj_pgm.dump_db_divida_ativa.schedules import (
#     divida_ativa_daily_update_schedule,
# )
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

rj_pgm_dump_db_divida_ativa_flow = deepcopy(dump_sql_flow)
rj_pgm_dump_db_divida_ativa_flow.name = (
    "PGM: DAM - divida ativa - Ingerir tabelas de banco SQL"
)
rj_pgm_dump_db_divida_ativa_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)

rj_pgm_dump_db_divida_ativa_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_PGM_AGENT_LABEL.value,  # label do agente
    ],
)

rj_pgm_dump_db_divida_ativa_default_parameters = {
    "db_database": "DAM_PRD",
    "db_host": "10.2.221.127",
    "db_port": "1433",
    "db_type": "sql_server",
    "dataset_id": "adm_financas_divida_ativa",
    "vault_secret_path": "dam-prod",
}

rj_pgm_dump_db_divida_ativa_flow = set_default_parameters(
    rj_pgm_dump_db_divida_ativa_flow,
    default_parameters=rj_pgm_dump_db_divida_ativa_default_parameters,
)

# rj_pgm_dump_db_divida_ativa_flow.schedule = divida_ativa_daily_update_schedule
