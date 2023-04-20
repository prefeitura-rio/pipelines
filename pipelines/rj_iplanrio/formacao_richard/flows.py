# -*- coding: utf-8 -*-
"""
Database dumping flows for sme project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_iplanrio.formacao_richard.schedules import (
    _1746_weekly_update_schedule,
)
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

#
# 1746 dump db flow
#
rj_iplanrio_formacao_richard_dump_1746_flow = deepcopy(dump_sql_flow)
rj_iplanrio_formacao_richard_dump_1746_flow.name = (
    "IPLANRIO: 1746 (formacao) - Ingerir tabelas de banco SQL"  # noqa
)
rj_iplanrio_formacao_richard_dump_1746_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
rj_iplanrio_formacao_richard_dump_1746_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
)

rj_iplanrio_formacao_richard_dump_1746_default_parameters = {
    "db_host": "10.70.1.34",
    "db_port": "1433",
    "db_type": "sql_server",
    "db_database": "REPLICA1746",
    "dataset_id": "formacao_richard",
    "vault_secret_path": "clustersql2",
}
rj_iplanrio_formacao_richard_dump_1746_flow = set_default_parameters(
    rj_iplanrio_formacao_richard_dump_1746_flow,
    default_parameters=rj_iplanrio_formacao_richard_dump_1746_default_parameters,
)

rj_iplanrio_formacao_richard_dump_1746_flow.schedule = _1746_weekly_update_schedule
