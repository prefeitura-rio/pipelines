# -*- coding: utf-8 -*-
"""
Flow Exemplo Carga DB EGPWEB Tables chances e comentarios para o Datalake
"""
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_iplanrio.formacao_edison_aula4a.schedules import (
    _EGPWEB_weekly_update_schedule,
)
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

rj_iplanrio_formacao_edison_dump_EGPWEB_flow = deepcopy(dump_sql_flow)
rj_iplanrio_formacao_edison_dump_EGPWEB_flow.name = (
    "IPLANRIO: EGPWEB - Formacao Edison - Ingerir tabela DB SQLSERVER"  # noqa"
)
rj_iplanrio_formacao_edison_dump_EGPWEB_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
rj_iplanrio_formacao_edison_dump_EGPWEB_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
)

rj_iplanrio_formacao_edison_dump_EGPWEB_default_parameters = {
    "db_database": "EGPWEB_PRD",
    "db_host": "10.2.221.101",
    "db_port": "1433",
    "db_type": "sql_server",
    "vault_secret_path": "egpweb-prod",
    "dataset_id": "formacao_egpweb_edison",
}

rj_iplanrio_formacao_edison_dump_EGPWEB_flow = set_default_parameters(
    rj_iplanrio_formacao_edison_dump_EGPWEB_flow,
    default_parameters=rj_iplanrio_formacao_edison_dump_EGPWEB_default_parameters,
)

rj_iplanrio_formacao_edison_dump_EGPWEB_flow.schedule = _EGPWEB_weekly_update_schedule
