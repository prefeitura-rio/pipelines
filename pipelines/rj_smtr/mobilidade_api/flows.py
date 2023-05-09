# -*- coding: utf-8 -*-
"""
Flows for mobilidade-api
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.rj_smtr.mobilidade_api.tasks import (
    get_gtfs_zipfile,
    extract_gtfs,
    execute_update,
    pre_treatment_mobilidade_api_gtfs,
    read_gtfs,
)
from pipelines.utils.decorators import Flow
from pipelines.constants import constants as emd_constants


with Flow("SMTR - Update DB - Postgres", code_owners=["caio"]) as update_db:

    zip_path = get_gtfs_zipfile()
    extracted_path = extract_gtfs(zip_path=zip_path)
    tables = read_gtfs(gtfs_path=extracted_path)
    table_paths = pre_treatment_mobilidade_api_gtfs(tables=tables)
    execute_update(table_paths=table_paths)

update_db.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
update_db.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
