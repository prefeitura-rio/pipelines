# -*- coding: utf-8 -*-
"""
Database dumping flows for iplanrio project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_segovi.dump_db_1746.schedules import _1746_daily_update_schedule
from pipelines.rj_iplanrio.formacao_joao.schedules import (
    _1746_weekly_update_schedule,
)
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

#
# 1746 dump db flow
#
rj_iplanrio_formacao_dump_flow = deepcopy(dump_sql_flow)
rj_iplanrio_formacao_dump_flow.name = (
    "IPLANRIO Formacao_Joao - Ingerir tabelas de banco SQL"
)
rj_iplanrio_formacao_dump_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_iplanrio_formacao_dump_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
)

rj_iplanrio_formacao_dump_default_parameters = {
    "db_database": "REPLICA1746",
    "db_host": "10.70.1.34",
    "db_port": "1433",
    "db_type": "sql_server",
    "dataset_id": "joao_aula4",  # destino
    "vault_secret_path": "clustersql2",  # credencial
    #   "lower_bound_date": "2021-01-01", #materialização
}
rj_iplanrio_formacao_dump_flow = set_default_parameters(
    rj_iplanrio_formacao_dump_flow,
    default_parameters=rj_iplanrio_formacao_dump_default_parameters,
)

rj_iplanrio_formacao_dump_flow.schedule = _1746_weekly_update_schedule
