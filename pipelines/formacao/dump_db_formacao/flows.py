# -*- coding: utf-8 -*-
"""
Database example dumping flow for infra training
"""
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_escritorio.dump_db_formacao.schedules import example_update_schedule
from pipelines.utils.dump_db_formacao.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

example_sql_flow = deepcopy(dump_sql_flow)
example_sql_flow.name = "EMD: Formação DataBase - Ingerir tabelas de banco SQL"
example_sql_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
example_sql_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
)

example_default_parameters = {
    "db_database": "EGPWEB_PRD",
    "db_host": "10.2.221.101",
    "db_port": "1433",
    "db_type": "sql_server",
    "vault_secret_path": "egpweb-prod",
    "dataset_id": "planejamento_gestao_acordo_resultados",
}

example_sql_flow = set_default_parameters(
    example_sql_flow, default_parameters=example_default_parameters
)

example_sql_flow.schedule = example_update_schedule