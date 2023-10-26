# -*- coding: utf-8 -*-
"""
Database dumping flows for segovi project (SISCOR)
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_seconserva.dump_db_siscor.schedules import (
    siscor_update_schedule,
)
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters


dump_siscor_flow = deepcopy(dump_sql_flow)
dump_siscor_flow.name = "SMI: SISCOR - Ingerir tabelas de banco SQL"
dump_siscor_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_siscor_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SECONSERVA_AGENT_LABEL.value,
    ],
)

siscor_default_parameters = {
    "db_database": "siscor_seconserva",
    "db_host": "10.70.11.61",
    "db_port": "1433",
    "db_type": "sql_server",
    "vault_secret_path": "db_siscor",
    "dataset_id": "infraestrutura_siscor_obras",
}
dump_siscor_flow = set_default_parameters(
    dump_siscor_flow, default_parameters=siscor_default_parameters
)

dump_siscor_flow.schedule = siscor_update_schedule
