# -*- coding: utf-8 -*-
"""
Database dumping flows for processorio
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# importa o schedule
# from pipelines.rj_iplanrio.processorio.schedules import (
#     processorio_infra_daily_update_schedule,
# )
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

#
# processorio dump db flow
#
rj_iplanrio_processorio_flow = deepcopy(dump_sql_flow)
rj_iplanrio_processorio_flow.name = (
    "IPLANRIO: processo.rio - Ingerir tabelas de banco SQL"
)
rj_iplanrio_processorio_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)

rj_iplanrio_processorio_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,  # label do agente
    ],
)

processorio_default_parameters = {
    "db_database": "SIGADOC.PCRJ",
    "db_host": "10.70.6.64",
    "db_port": "1521",
    "db_type": "oracle",
    "dataset_id": "administracao_servicos_publicos",
    "vault_secret_path": "processorio-prod",
}

rj_iplanrio_processorio_flow = set_default_parameters(
    rj_iplanrio_processorio_flow,
    default_parameters=processorio_default_parameters,
)

# rj_iplanrio_processorio_flow.schedule = processorio_infra_daily_update_schedule
