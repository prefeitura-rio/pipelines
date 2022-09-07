# -*- coding: utf-8 -*-
# pylint: disable=C0103

"""
Flows for meteorologia/refletividade_horizontal_radar
"""


from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.refletividade_horizontal_radar.constants import (
    constants as radar_constantes,
)
from pipelines.rj_cor.meteorologia.refletividade_horizontal_radar.schedules import (
    minute_schedule,
)
from pipelines.rj_cor.meteorologia.refletividade_horizontal_radar.tasks import (
    download,
    get_on_redis,
    upload_gcp,
    save_on_redis,
)
from pipelines.utils.decorators import Flow

with Flow(
    "COR: Meteorologia - Refletividade Horizontal INEA",
    code_owners=[
        "paty",
    ],
) as rj_cor_meteorologia_refletividade_horizontal_radar:

    dataset_id = radar_constantes.DATASET_ID.value
    table_id = radar_constantes.TABLE_ID.value

    redis_files = get_on_redis(dataset_id, table_id, mode="dev")
    local_files_path = download(redis_files)
    files = upload_gcp(
        dataset_id, table_id, local_files_path, redis_files, wait=redis_files
    )
    save_on_redis(dataset_id, table_id, "dev", files, wait=files)

rj_cor_meteorologia_refletividade_horizontal_radar.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
rj_cor_meteorologia_refletividade_horizontal_radar.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
rj_cor_meteorologia_refletividade_horizontal_radar.schedule = minute_schedule
