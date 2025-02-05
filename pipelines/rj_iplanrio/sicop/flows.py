# -*- coding: utf-8 -*-
"""
Database dumping flows for processo.rio sicop
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# importa o schedule
# from pipelines.rj_iplanrio.sicop.schedules import (
#     sicop_infra_daily_update_schedule,
# )
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

#
# sicop dump db flow
#
rj_iplanrio_sicop_flow = deepcopy(dump_sql_flow)
rj_iplanrio_sicop_flow.name = (
    "IPLANRIO: processo.rio - SICOP - Ingerir tabelas de banco SQL"
)
rj_iplanrio_sicop_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_iplanrio_sicop_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,  # label do agente
    ],
)

sicop_default_parameters = {
    "db_database": "CP01.smf",
    "db_host": "10.90.31.22",
    "db_port": "1521",
    "db_type": "oracle",
    "dataset_id": "adm_processo_interno_sicop",
    "vault_secret_path": "sicop-sql",
}
rj_iplanrio_sicop_flow = set_default_parameters(
    rj_iplanrio_sicop_flow,
    default_parameters=sicop_default_parameters,
)

# rj_iplanrio_sicop_flow.schedule = sicop_infra_daily_update_schedule
