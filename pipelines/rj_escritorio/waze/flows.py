# -*- coding: utf-8 -*-
"""
Flows for emd
"""

from functools import partial

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
import pendulum

from pipelines.constants import constants
from pipelines.rj_escritorio.waze.tasks import (
    load_geometries,
    fecth_waze,
    normalize_data,
    upload_to_native_table,
)
from pipelines.rj_escritorio.waze.schedules import every_five_minutes
from pipelines.utils.utils import notify_discord_on_failure
from pipelines.utils.tasks import rename_current_flow_run


with Flow(
    name="EMD: escritorio - Alertas Waze",
    on_failure=partial(
        notify_discord_on_failure,
        secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value,
    ),
) as flow:
    dataset_id = "transporte_rodoviario_waze"
    table_id = "alertas"

    #####################################
    #
    # Rename flow run
    #
    #####################################
    now = pendulum.now(pendulum.timezone("America/Sao_Paulo"))
    now = f"{now.hour}:{f'0{now.minute}' if len(str(now.minute))==1 else now.minute}"
    rename_flow_run = rename_current_flow_run(msg=f"Waze: {now}")

    areas = load_geometries()

    res = fecth_waze(areas=areas)

    df = normalize_data(responses=res)

    upload_to_native_table(
        dataset_id=dataset_id,
        table_id=table_id,
        dataframe=df,
    )

flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = every_five_minutes
