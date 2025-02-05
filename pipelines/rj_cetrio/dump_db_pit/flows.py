# -*- coding: utf-8 -*-
"""
Database dumping flows for segovi project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.rj_cetrio.dump_db_pit.schedules import (
#     cetrio_daily_update_schedule,
# )
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

cetrio_flow = deepcopy(dump_sql_flow)
cetrio_flow.name = "CETRIO: OCR - Ingerir tabelas de banco SQL"
cetrio_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cetrio_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CETRIO_AGENT_LABEL.value,
    ],
)

cetrio_default_parameters = {
    "db_database": "DWOCR_Staging",
    "db_host": "10.39.64.50",
    "db_port": "1433",
    "db_type": "sql_server",
    "vault_secret_path": "cet-rio-ocr-staging",
    "dataset_id": "transporte_rodoviario_radar_transito",
    "mode": "dev",
}
cetrio_flow = set_default_parameters(
    cetrio_flow, default_parameters=cetrio_default_parameters
)

# cetrio_flow.schedule = cetrio_daily_update_schedule
