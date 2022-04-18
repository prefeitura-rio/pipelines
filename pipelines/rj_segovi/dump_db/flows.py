# -*- coding: utf-8 -*-
"""
Database dumping flows for sme project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_segovi.dump_db.schedules import _1746_daily_update_schedule
from pipelines.utils.dump_db.flows import dump_sql_flow


dump_1746_flow = deepcopy(dump_sql_flow)
dump_1746_flow.name = "SEGOVI: 1746 - Ingerir tabelas de banco SQL"
dump_1746_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_1746_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
dump_1746_flow.schedule = _1746_daily_update_schedule
