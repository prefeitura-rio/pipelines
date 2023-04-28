# -*- coding: utf-8 -*-
"""
Flows for mobilidade-api
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.rj_smtr.mobilidade_api.tasks import (
    get_gtfs_zipfiles,
    extract_gtfs,
    execute_update,
    treat_gtfs_tables,
    concat_gtfs,
)
from pipelines.utils.decorators import Flow
from pipelines.constants import constants as emd_constants


with Flow("SMTR - Update DB - Postgres", code_owners=["caio"]) as update_db:

    gtfs_paths = get_gtfs_zipfiles()
    gtfs_dirs = extract_gtfs.map(zip_path=gtfs_paths)
    tables = concat_gtfs(dir_paths=gtfs_dirs)  # change args to single list?
    table_paths = treat_gtfs_tables(tables=tables)
    execute_update(table_paths=table_paths)

update_db.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
update_db.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
