# -*- coding: utf-8 -*-
"""
Database dumping flows for segovi project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_smfp.dump_db.schedules import (
    ergon_monthly_update_schedule,
)
from pipelines.utils.dump_db.flows import dump_sql_flow


dump_ergon_flow = deepcopy(dump_sql_flow)
dump_ergon_flow.name = "EMD: ergon - Ingerir tabelas de banco SQL"
dump_ergon_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_ergon_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
dump_ergon_flow.schedule = ergon_monthly_update_schedule
