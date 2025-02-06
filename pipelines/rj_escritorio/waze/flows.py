# -*- coding: utf-8 -*-
"""
Flows for emd.
"""

# pylint: disable=C0103

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
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import rename_current_flow_run_now_time, get_now_time


with Flow(
    name="EMD: escritorio - Alertas Waze",
) as flow:
    dataset_id = "transporte_rodoviario_waze"
    table_id = "alertas"

    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_now_time(
        prefix="Waze: ", now_time=get_now_time()
    )

    areas = load_geometries(wait=rename_flow_run)

    responses = fecth_waze(areas=areas, wait=areas)

    dataframe = normalize_data(responses=responses, wait=responses)

    upload_to_native_table(
        dataset_id=dataset_id,
        table_id=table_id,
        dataframe=dataframe,
        wait=dataframe,
    )

flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
flow.schedule = every_five_minutes
