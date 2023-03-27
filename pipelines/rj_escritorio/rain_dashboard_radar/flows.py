# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows for setting rain dashboard using radar data.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants


# from pipelines.rj_escritorio.rain_dashboard.constants import (
#     constants as rain_dashboard_constants,
# )

# from pipelines.rj_escritorio.rain_dashboard.schedules import every_fifteen_minutes
from pipelines.rj_escritorio.rain_dashboard_radar.tasks import (
    change_predict_rain_specs,
    download_files_storage,
    get_filenames_storage,
)
from pipelines.utils.decorators import Flow

with Flow(
    name="EMD: Extrair e atualizar dados de chuva na api.dados.rio",
    code_owners=[
        "paty",
    ],
    skip_if_running=True,
) as rj_escritorio_rain_dashboard_radar_flow:
    # Parameters
    mode = Parameter("mode", default="prod")
    radar = Parameter("radar_name", default="gua")

    # Tasks
    bucket_name = "rj-escritorio-dev"
    files_on_storage_list = get_filenames_storage(bucket_name, radar)
    download_files_storage(
        bucket_name,
        files_to_download=files_on_storage_list,
        destination_path="radar_data/",
    )
    change_predict_rain_specs(
        files_to_model=files_on_storage_list, destination_path="radar_data/"
    )


rj_escritorio_rain_dashboard_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio_rain_dashboard_radar_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
