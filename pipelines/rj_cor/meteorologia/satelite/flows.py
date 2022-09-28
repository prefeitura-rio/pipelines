# -*- coding: utf-8 -*-
"""
Flows for emd
"""
from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.rj_cor.meteorologia.satelite.tasks import (
    get_dates,
    slice_data,
    download,
    tratar_dados,
    salvar_parquet,
)
from pipelines.rj_cor.meteorologia.satelite.schedules import hour_schedule
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
)

with Flow(
    name="COR: Meteorologia - Satelite GOES 16",
    code_owners=[
        "paty",
    ],
) as cor_meteorologia_goes16:

    CURRENT_TIME = get_dates()

    ano, mes, dia, hora, dia_juliano = slice_data(current_time=CURRENT_TIME)

    DUMP_MODE = "append"

    # Materialization parameters
    MATERIALIZE_AFTER_DUMP = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    MATERIALIZE_TO_DATARIO = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    MATERIALIZATION_MODE = Parameter("mode", default="dev", required=False)

    # Para taxa de precipitação
    VARIAVEL_RR = "RRQPEF"
    DATASET_ID_RR = "meio_ambiente_clima"
    TABLE_ID_RR = "taxa_precipitacao_satelite"

    # Flow da taxa de precipitação
    filename_rr = download(
        variavel=VARIAVEL_RR, ano=ano, dia_juliano=dia_juliano, hora=hora
    )

    info_rr = tratar_dados(filename=filename_rr)
    path_rr = salvar_parquet(info=info_rr)

    # Create table in BigQuery
    UPLOAD_TABLE_RR = create_table_and_upload_to_gcs(
        data_path=path_rr,
        dataset_id=DATASET_ID_RR,
        table_id=TABLE_ID_RR,
        dump_mode=DUMP_MODE,
        wait=path_rr,
    )

    # Para quantidade de água precipitável
    VARIAVEL_TPW = "TPWF"
    DATASET_ID_TPW = "meio_ambiente_clima"
    TABLE_ID_TPW = "quantidade_agua_precipitavel_satelite"

    filename_tpw = download(
        variavel=VARIAVEL_TPW, ano=ano, dia_juliano=dia_juliano, hora=hora
    )

    info_tpw = tratar_dados(filename=filename_tpw)
    path_tpw = salvar_parquet(info=info_tpw)

    UPLOAD_TABLE_TPW = create_table_and_upload_to_gcs(
        data_path=path_tpw,
        dataset_id=DATASET_ID_TPW,
        table_id=TABLE_ID_TPW,
        dump_mode=DUMP_MODE,
        wait=path_tpw,
    )

    # para clean_ir_longwave_window (band 13) CMIPF
    # Para quantidade de água precipitável
    VARIAVEL_cmip = "CMIPF"
    DATASET_ID_cmip = "clima_satelite"
    TABLE_ID_cmip = "clean_ir_longwave_window_goes_16"

    filename_cmip = download(
        variavel=VARIAVEL_cmip, ano=ano, dia_juliano=dia_juliano, hora=hora, band="13"
    )

    info_cmip = tratar_dados(filename=filename_cmip)
    path_cmip = salvar_parquet(info=info_cmip)

    UPLOAD_TABLE_cmip = create_table_and_upload_to_gcs(
        data_path=path_cmip,
        dataset_id=DATASET_ID_cmip,
        table_id=TABLE_ID_cmip,
        dump_mode=DUMP_MODE,
        wait=path_cmip,
    )

    # Trigger DBT flow run
    with case(MATERIALIZE_AFTER_DUMP, True):
        current_flow_labels = get_current_flow_labels()

        # Materializar RR
        materialization_flow_rr = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": DATASET_ID_RR,
                "table_id": TABLE_ID_RR,
                "mode": MATERIALIZATION_MODE,
                "materialize_to_datario": MATERIALIZE_TO_DATARIO,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {DATASET_ID_RR}.{TABLE_ID_RR}",
        )

        materialization_flow_rr.set_upstream(UPLOAD_TABLE_RR)

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
                "dataset_id": DATASET_ID_TPW,
                "table_id": TABLE_ID_TPW,
                "mode": MATERIALIZATION_MODE,
                "materialize_to_datario": MATERIALIZE_TO_DATARIO,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {DATASET_ID_TPW}.{TABLE_ID_TPW}",
        )

        materialization_flow_tpw.set_upstream(UPLOAD_TABLE_TPW)

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
                "dataset_id": DATASET_ID_cmip,
                "table_id": TABLE_ID_cmip,
                "mode": MATERIALIZATION_MODE,
                "materialize_to_datario": MATERIALIZE_TO_DATARIO,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {DATASET_ID_cmip}.{TABLE_ID_cmip}",
        )

        materialization_flow_cmip.set_upstream(UPLOAD_TABLE_cmip)

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
