# -*- coding: utf-8 -*-
"""
Flow Exemplo Carga DB 1746 e Table chamados para o Datalake
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_iplanrio.formacao_edison_aula4.schedules import (
    _1746_weekly_update_schedule,
)
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

#
# 1746 dump db flow
#
rj_iplanrio_formacao_edison_dump_1746_flow = deepcopy(dump_sql_flow)
rj_iplanrio_formacao_edison_dump_1746_flow.name = (
    "IPLANRIO: 1746 - Formacao Edison - Ingerir tabela DB SQLSERVER"  # noqa
)
rj_iplanrio_formacao_edison_dump_1746_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
rj_iplanrio_formacao_edison_dump_1746_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
)

rj_iplanrio_formacao_edison_dump_1746_default_parameters = {
    # parametros da origem/fonte
    "db_host": "10.70.1.34",
    "db_port": "1433",
    "db_type": "sql_server",
    "db_database": "REPLICA1746",
    # parametros do destino
    "dataset_id": "formacao_1746_edison",
    # table_id = chamados
    # credenciais
    "vault_secret_path": "clustersql2",
}
rj_iplanrio_formacao_edison_dump_1746_flow = set_default_parameters(
    rj_iplanrio_formacao_edison_dump_1746_flow,
    default_parameters=rj_iplanrio_formacao_edison_dump_1746_default_parameters,
)

rj_iplanrio_formacao_edison_dump_1746_flow.schedule = _1746_weekly_update_schedule
