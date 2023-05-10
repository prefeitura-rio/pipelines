# -*- coding: utf-8 -*-
"""
Database dumping flows for processorio formacao infra project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# importa o schedule
from pipelines.rj_iplanrio import _processorio_infra_daily_update_schedule
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

#
# processorio dump db flow
#
dump_processorio_infra = deepcopy(dump_sql_flow)
dump_processorio_infra.name = "Processorio - Ingerir tabelas de banco SQL"
dump_processorio_infra.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_processorio_infra.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,  # label do agente
    ],
)

_processorio__infra_default_parameters = {
    "db_database": "REPLICA1746",
    "db_host": "10.70.1.34",
    "db_port": "1433",
    "db_type": "sql_server",
    "dataset_id": "administracao_servicos_publicos",
    "vault_secret_path": "clustersql2",
    "lower_bound_date": "2021-01-01",
}
dump_processorio_infra = set_default_parameters(
    dump_processorio_infra, default_parameters=_processorio__infra_default_parameters
)

dump_processorio_infra.schedule = _processorio_infra_daily_update_schedule
