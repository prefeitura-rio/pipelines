# -*- coding: utf-8 -*-
"""
Database dumping flows for smfp ergon comlurb
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_smfp.dump_db_ergon_comlurb.schedules import (
#     ergon_comlurb_monthly_update_schedule,
# )
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters


dump_ergon_flow = deepcopy(dump_sql_flow)
dump_ergon_flow.name = "SMFP: ergon comlurb - Ingerir tabelas de banco SQL"
dump_ergon_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_ergon_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
)

ergon_default_parameters = {
    "db_database": "P25",
    "db_host": "10.70.6.26",
    "db_port": "1521",
    "db_type": "oracle",
    "vault_secret_path": "ergon-comlurb",
    "dataset_id": "recursos_humanos_ergon_comlurb",
}
dump_ergon_flow = set_default_parameters(
    dump_ergon_flow, default_parameters=ergon_default_parameters
)

# dump_ergon_flow.schedule = ergon_comlurb_monthly_update_schedule
