# -*- coding: utf-8 -*-
# flake8: noqa: E501
# pylint: disable=C0103
"""
Flows for setting rain dashboard using radar data.
"""
from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.radar.precipitacao.schedules import TIME_SCHEDULE
from pipelines.rj_cor.meteorologia.radar.precipitacao.constants import (
    constants as radar_constants,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.tasks import (
    change_predict_rain_specs,
    download_files_storage,
    get_filenames_storage,
    run_model,
)
from pipelines.rj_cor.tasks import (
    get_on_redis,
    save_on_redis,
)
from pipelines.rj_escritorio.rain_dashboard.constants import (
    constants as rain_dashboard_constants,
)
from pipelines.utils.decorators import Flow

from pipelines.utils.tasks import create_table_and_upload_to_gcs


with Flow(
    name="COR: Meteorologia - Precipitacao RADAR",
    code_owners=[
        "paty",
    ],
    # skip_if_running=True,
) as cor_meteorologia_precipitacao_radar_flow:

    # Prefect Parameters
    MODE = Parameter("mode", default="prod")
    RADAR_NAME = Parameter("radar_name", default="gua")
    TRIGGER_RAIN_DASHBOARD_UPDATE = Parameter(
        "trigger_rain_dashboard_update", default=True, required=False
    )

    # Other Parameters
    DATASET_ID = radar_constants.DATASET_ID.value
    TABLE_ID = radar_constants.TABLE_ID.value
    DUMP_MODE = "append"
    BASE_PATH = "pipelines/rj_cor/meteorologia/radar/precipitacao/"

    # Tasks
    BUCKET_NAME = "rj-escritorio-dev"
    files_saved_redis = get_on_redis(DATASET_ID, TABLE_ID, mode=MODE)
    files_on_storage_list = get_filenames_storage(
        BUCKET_NAME, RADAR_NAME, files_saved_redis
    )

    download_files_task = download_files_storage(
        bucket_name=BUCKET_NAME,
        files_to_download=files_on_storage_list,
        destination_path=f"{BASE_PATH}radar_data/",
    )
    change_json_task = change_predict_rain_specs(
        files_to_model=files_on_storage_list,
        destination_path=f"{BASE_PATH}radar_data/",
    )
    download_files_task.set_upstream(change_json_task)
    run_model_task = run_model()
    run_model_task.set_upstream(download_files_task)
    # run_model_task.set_upstream(change_json_task)

    upload_table = create_table_and_upload_to_gcs(
        data_path=f"{BASE_PATH}predictions/",
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_mode=DUMP_MODE,
        wait=run_model_task,
    )
    # upload_table.set_upstream(run_model_task)

    # Save new filenames on redis
    save_last_update_redis = save_on_redis(
        DATASET_ID,
        TABLE_ID,
        MODE,
        files_on_storage_list,
        keep_last=3,
        wait=upload_table,
    )
    # save_last_update_redis.set_upstream(upload_table)

    with case(TRIGGER_RAIN_DASHBOARD_UPDATE, True):
        # Trigger rain dashboard update flow run
        rain_radar_dashboard_update_flow = create_flow_run(
            flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters=radar_constants.RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
            labels=[
                "rj-cor",
            ],
            run_name="Update radar rain dashboard data (triggered by precipitacao_radar flow)",  # noqa
        )
        rain_radar_dashboard_update_flow.set_upstream(upload_table)

        wait_for_rain_dashboard_update = wait_for_flow_run(
            flow_run_id=rain_radar_dashboard_update_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=False,
        )

cor_meteorologia_precipitacao_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_precipitacao_radar_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)

cor_meteorologia_precipitacao_radar_flow.schedule = TIME_SCHEDULE
