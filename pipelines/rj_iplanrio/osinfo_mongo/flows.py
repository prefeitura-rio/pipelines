# -*- coding: utf-8 -*-
"""
Database dumping flows for OSINFO MongoDB
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_iplanrio.osinfo_mongo.schedules import (
    osinfo_mongo_files_daily_schedule,
    osinfo_mongo_files_chunks_daily_schedule,
)
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

# OSINFO MongoDB dump flow - files table
rj_iplanrio_osinfo_mongo_files_flow = deepcopy(dump_sql_flow)
rj_iplanrio_osinfo_mongo_files_flow.name = (
    "IPLANRIO: OSINFO MongoDB - Ingerir tabela files"
)
rj_iplanrio_osinfo_mongo_files_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)

rj_iplanrio_osinfo_mongo_files_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
)

osinfo_mongo_files_default_parameters = {
    "db_database": "OSINFO_FILES",
    "db_host": "187.111.98.189",
    "db_port": "27017",
    "db_type": "mongodb",
    "dataset_id": "osinfo_mongo",
    "table_id": "files",
    "vault_secret_path": "osinfo-mongo",  # TODO: confirmar secret path!
    "execute_query": "FILES.files",  # MongoDB collection name
}

rj_iplanrio_osinfo_mongo_files_flow = set_default_parameters(
    rj_iplanrio_osinfo_mongo_files_flow,
    default_parameters=osinfo_mongo_files_default_parameters,
)

# rj_iplanrio_osinfo_mongo_files_flow.schedule = osinfo_mongo_files_daily_schedule

# OSINFO MongoDB dump flow - files_chunks table
rj_iplanrio_osinfo_mongo_files_chunks_flow = deepcopy(dump_sql_flow)
rj_iplanrio_osinfo_mongo_files_chunks_flow.name = (
    "IPLANRIO: OSINFO MongoDB - Ingerir tabela files_chunks"
)
rj_iplanrio_osinfo_mongo_files_chunks_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)

rj_iplanrio_osinfo_mongo_files_chunks_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
)

osinfo_mongo_files_chunks_default_parameters = {
    "db_database": "OSINFO_FILES",
    "db_host": "187.111.98.189",
    "db_port": "27017",
    "db_type": "mongodb",
    "dataset_id": "osinfo_mongo",
    "table_id": "files_chunks",
    "vault_secret_path": "osinfo-mongo",
    "execute_query": "FILES.chunks",  # MongoDB collection name
}

rj_iplanrio_osinfo_mongo_files_chunks_flow = set_default_parameters(
    rj_iplanrio_osinfo_mongo_files_chunks_flow,
    default_parameters=osinfo_mongo_files_chunks_default_parameters,
)

# rj_iplanrio_osinfo_mongo_files_chunks_flow.schedule = osinfo_mongo_files_chunks_daily_schedule
