# -*- coding: utf-8 -*-
"""
Database dumping flows for SME project..
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_sme.dump_db_educacao_basica.schedules import (
#     sme_educacao_basica_daily_update_schedule,
# )
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

dump_sme_flow = deepcopy(dump_sql_flow)
dump_sme_flow.name = "SME: educacao_basica - Ingerir tabelas de banco SQL"
dump_sme_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)

dump_sme_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SME_AGENT_LABEL.value,
    ],
)

sme_default_parameters = {
    "db_database": "GestaoEscolar",
    "db_host": "10.70.6.103",
    "db_port": "1433",
    "db_type": "sql_server",
    "vault_secret_path": "clustersqlsme",
    "dataset_id": "educacao_basica",
}
dump_sme_flow = set_default_parameters(
    dump_sme_flow, default_parameters=sme_default_parameters
)

# dump_sme_flow.schedule = sme_educacao_basica_daily_update_schedule
