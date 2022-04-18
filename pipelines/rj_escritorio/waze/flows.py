# -*- coding: utf-8 -*-
"""
Flows for emd
"""

from functools import partial

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.waze.tasks import (
    load_geometries,
    fecth_waze,
    normalize_data,
    upload_to_native_table,
)
from pipelines.rj_escritorio.waze.schedules import every_five_minutes
from pipelines.utils.utils import notify_discord_on_failure

# from pipelines.emd.template_pipeline.schedules import every_two_weeks

with Flow(
    name="EMD: escritorio - Alertas Waze",
    on_failure=partial(
        notify_discord_on_failure,
        secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value,
    ),
) as flow:

    areas = load_geometries()

    res = fecth_waze(areas=areas)

    df = normalize_data(responses=res)

    upload_to_native_table(
        dataset_id="transporte_rodoviario_waze",
        table_id="alertas",
        dataframe=df,
    )

flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = every_five_minutes
