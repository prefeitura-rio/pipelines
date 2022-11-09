# -*- coding: utf-8 -*-
"""
Database dumping flows for segovi project 
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_smfp.dump_db_ergon.schedules import (
    ergon_monthly_update_schedule,
)
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters


dump_sql_ergon_flow = deepcopy(dump_sql_flow)
dump_sql_ergon_flow.name = "SMFP: ergon - Ingerir tabelas de banco SQL"
dump_sql_ergon_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sql_ergon_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
)

ergon_default_parameters = {
    "db_database": "P01.PCRJ",
    "db_host": "10.70.6.21",
    "db_port": "1526",
    "db_type": "oracle",
    "vault_secret_path": "ergon-prod",
    "dataset_id": "recursos_humanos_ergon",
}
dump_sql_ergon_flow = set_default_parameters(
    dump_sql_ergon_flow, default_parameters=ergon_default_parameters
)

dump_sql_ergon_flow.schedule = ergon_monthly_update_schedule
