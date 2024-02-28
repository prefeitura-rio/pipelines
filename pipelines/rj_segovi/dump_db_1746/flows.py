# -*- coding: utf-8 -*-
"""
Database dumping flows for sme project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_segovi.dump_db_1746.schedules import _1746_daily_update_schedule
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

#
# 1746 dump db flow
#
dump_1746_flow = deepcopy(dump_sql_flow)
dump_1746_flow.name = "SEGOVI: 1746 - Ingerir tabelas de banco SQL"
dump_1746_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_1746_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
)

_1746_default_parameters = {
    "db_database": "REPLICA1746",
    "db_host": "10.70.1.34",
    "db_port": "1433",
    "db_type": "sql_server",
    "dataset_id": "adm_central_atendimento_1746",
    "vault_secret_path": "clustersql2",
    "lower_bound_date": "2021-01-01",
}
dump_1746_flow = set_default_parameters(
    dump_1746_flow, default_parameters=_1746_default_parameters
)

dump_1746_flow.schedule = _1746_daily_update_schedule
