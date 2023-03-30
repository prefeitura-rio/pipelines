# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa: E501
"""
Flows for setting rain dashboard using radar data.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants

# from pipelines.rj_cor.meteorologia.radar.rain_dashboard.schedules import every_fifteen_minutes
from pipelines.rj_cor.meteorologia.radar.rain_dashboard.constants import (
    constants as radar_constants,
)
from pipelines.rj_cor.meteorologia.radar.rain_dashboard.tasks import (
    change_predict_rain_specs,
    download_files_storage,
    get_filenames_storage,
    run_model,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import create_table_and_upload_to_gcs


with Flow(
    name="EMD: Extrair e atualizar dados de chuva na api.dados.rio",
    code_owners=[
        "paty",
    ],
    # skip_if_running=True,
) as rj_cor_rain_dashboard_radar_flow:
    # Prefect Parameters
    mode = Parameter("mode", default="prod")
    radar = Parameter("radar_name", default="gua")

    # Other Parameters
    dataset_id = radar_constants.DATASET_ID.value
    table_id = radar_constants.TABLE_ID.value
    dump_mode = "append"
    base_path = "pipelines/rj_cor/meteorologia/radar/rain_dashboard/"

    # Tasks
    bucket_name = "rj-escritorio-dev"
    files_on_storage_list = get_filenames_storage(bucket_name, radar)
    download_files_task = download_files_storage(
        bucket_name=bucket_name,
        files_to_download=files_on_storage_list,
        destination_path=f"{base_path}radar_data/",
    )
    change_json_task = change_predict_rain_specs(
        files_to_model=files_on_storage_list,
        destination_path=f"{base_path}radar_data/",
    )
    run_model_task = run_model()
    run_model_task.set_upstream(download_files_task)

    upload_table = create_table_and_upload_to_gcs(
        data_path=f"{base_path}predictions/",
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )
    upload_table.set_upstream(run_model_task)

rj_cor_rain_dashboard_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_cor_rain_dashboard_radar_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
