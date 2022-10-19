# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows for emd
"""
from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.satelite.constants import (
    constants as satelite_constants,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.rj_cor.meteorologia.satelite.tasks import (
    checa_update,
    get_dates,
    slice_data,
    download,
    tratar_dados,
    save_data,
)
from pipelines.rj_cor.tasks import (
    get_on_redis,
    save_on_redis,
)
from pipelines.rj_cor.meteorologia.satelite.schedules import hour_schedule

from pipelines.utils.decorators import Flow

# from prefect import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
)

with Flow(
    name="COR: Meteorologia - Satelite GOES 16"
    # code_owners=[
    #     "paty",
    # ],
) as cor_meteorologia_goes16:

    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    materialization_mode = Parameter("mode", default="dev", required=False)

    # Other parameters
    dataset_id = satelite_constants.DATASET_ID.value
    dump_mode = "append"
    ref_filename = Parameter("ref_filename", default=None, required=False)
    current_time = Parameter("current_time", default=None, required=False)
    current_time = get_dates(current_time)

    date_hour_info = slice_data(current_time=current_time, ref_filename=ref_filename)

    # Para taxa de precipitação
    variavel_rr = satelite_constants.VARIAVEL_RR.value
    table_id_rr = satelite_constants.TABLE_ID_RR.value

    # Get filenames that were already treated on redis
    redis_files_rr = get_on_redis(dataset_id, table_id_rr, mode="prod")

    # Download raw data from API
    filename_rr, redis_files_rr_updated = download(
        variavel=variavel_rr,
        date_hour_info=date_hour_info,
        redis_files=redis_files_rr,
        ref_filename=ref_filename,
        wait=redis_files_rr,
    )

    # Check if there is new files on API
    update_rr = checa_update(redis_files_rr, redis_files_rr_updated)

    # Start data treatment if there are new files
    with case(update_rr, True):
        info_rr = tratar_dados(filename=filename_rr)
        path_rr = save_data(info=info_rr, file_path=filename_rr)

        # Create table in BigQuery
        upload_table_rr = create_table_and_upload_to_gcs(
            data_path=path_rr,
            dataset_id=dataset_id,
            table_id=table_id_rr,
            dump_mode=dump_mode,
            wait=path_rr,
        )

        # Save new filenames on redis
        save_on_redis(
            dataset_id,
            table_id_rr,
            "prod",
            redis_files_rr_updated,
            wait=path_rr,
        )

    # Para quantidade de água precipitável
    variavel_tpw = satelite_constants.VARIAVEL_TPW.value
    table_id_tpw = satelite_constants.TABLE_ID_TPW.value

    # Get filenames that were already treated on redis
    redis_files_tpw = get_on_redis(dataset_id, table_id_tpw, mode="prod")

    # Download raw data from API
    filename_tpw, redis_files_tpw_updated = download(
        variavel=variavel_tpw,
        date_hour_info=date_hour_info,
        redis_files=redis_files_tpw,
        ref_filename=ref_filename,
        wait=redis_files_tpw,
    )

    # Check if there is new files on API
    update_tpw = checa_update(redis_files_tpw, redis_files_tpw_updated)

    # Start data treatment if there are new files
    with case(update_tpw, True):
        info_tpw = tratar_dados(filename=filename_tpw)
        path_tpw = save_data(info=info_tpw, file_path=filename_tpw)

        upload_table_tpw = create_table_and_upload_to_gcs(
            data_path=path_tpw,
            dataset_id=dataset_id,
            table_id=table_id_tpw,
            dump_mode=dump_mode,
            wait=path_tpw,
        )

        # Save new filenames on redis
        save_on_redis(
            dataset_id,
            table_id_tpw,
            "prod",
            redis_files_tpw_updated,
            wait=path_tpw,
        )

    # Para clean_ir_longwave_window (band 13) CMIPF
    variavel_cmip = satelite_constants.VARIAVEL_cmip.value
    table_id_cmip = satelite_constants.TABLE_ID_cmip.value

    # Get filenames that were already treated on redis
    redis_files_cmip = get_on_redis(dataset_id, table_id_cmip, mode="prod")

    # Download raw data from API
    filename_cmip, redis_files_cmip_updated = download(
        variavel=variavel_cmip,
        date_hour_info=date_hour_info,
        band="13",
        redis_files=redis_files_cmip,
        ref_filename=ref_filename,
        wait=redis_files_cmip,
    )

    # Check if there is new files on API
    update_cmip = checa_update(redis_files_cmip, redis_files_cmip_updated)

    # Start data treatment if there are new files
    with case(update_cmip, True):
        info_cmip = tratar_dados(filename=filename_cmip)
        path_cmip = save_data(info=info_cmip, file_path=filename_cmip)

        # Create table in BigQuery
        upload_table_cmip = create_table_and_upload_to_gcs(
            data_path=path_cmip,
            dataset_id=dataset_id,
            table_id=table_id_cmip,
            dump_mode=dump_mode,
            wait=path_cmip,
        )

        # Save new filenames on redis
        save_on_redis(
            dataset_id,
            table_id_cmip,
            "prod",
            redis_files_cmip_updated,
            wait=path_cmip,
        )

    # Trigger DBT flow run
    with case(materialize_after_dump, True):
        current_flow_labels = get_current_flow_labels()

        # Materializar RR
        materialization_flow_rr = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id_rr,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id_rr}",
        )

        materialization_flow_rr.set_upstream(upload_table_rr)

        wait_for_materialization_rr = wait_for_flow_run(
            materialization_flow_rr,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        # Materializar TPW
        materialization_flow_tpw = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id_tpw,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id_tpw}",
        )

        materialization_flow_tpw.set_upstream(upload_table_tpw)

        wait_for_materialization_tpw = wait_for_flow_run(
            materialization_flow_tpw,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        # Materializar CMIP
        materialization_flow_cmip = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id_cmip,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id_cmip}",
        )

        materialization_flow_cmip.set_upstream(upload_table_cmip)

        wait_for_materialization_cmip = wait_for_flow_run(
            materialization_flow_cmip,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

# para rodar na cloud
cor_meteorologia_goes16.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16.schedule = hour_schedule
