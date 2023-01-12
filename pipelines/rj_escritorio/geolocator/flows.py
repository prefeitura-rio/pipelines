# -*- coding: utf-8 -*-
# pylint: disable="unexpected-keyword-arg", C0103

"""
Flows for geolocator
"""

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.rj_escritorio.geolocator.constants import (
    constants as geolocator_constants,
)
from pipelines.rj_escritorio.geolocator.schedules import every_day_at_four_am
from pipelines.rj_escritorio.geolocator.tasks import (  # get_today,
    cria_csv,
    seleciona_enderecos_novos,
    geolocaliza_enderecos,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.tasks import (
    get_current_flow_labels,
    create_table_and_upload_to_gcs,
)

with Flow(
    "EMD: escritorio - Geolocalizacao de chamados 1746",
    code_owners=[
        "paty",
    ],
) as daily_geolocator_flow:
    # [enderecos_conhecidos, enderecos_ontem, chamados_ontem, base_enderecos_atual]
    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    dataset_id = geolocator_constants.DATASET_ID.value
    table_id = geolocator_constants.TABLE_ID.value

    novos_enderecos, possui_enderecos_novos = seleciona_enderecos_novos()

    # Roda apenas se houver endereços novos
    with case(possui_enderecos_novos, True):
        base_geolocalizada = geolocaliza_enderecos(
            base_enderecos_novos=novos_enderecos, upstream_tasks=[novos_enderecos]
        )
        base_path = cria_csv(  # pylint: disable=invalid-name
            base_enderecos_novos=base_geolocalizada,
            upstream_tasks=[base_geolocalizada],
        )
        # today = get_today()
        upload_table = create_table_and_upload_to_gcs(
            data_path=base_path,
            dataset_id=geolocator_constants.DATASET_ID.value,
            table_id=geolocator_constants.TABLE_ID.value,
            dump_mode="append",
            wait=base_path,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "materialize_to_datario": materialize_to_datario,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
            )
            materialization_flow.set_upstream(upload_table)

            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )

            with case(dump_to_gcs, True):
                # Trigger Dump to GCS flow run with project id as datario
                dump_to_gcs_flow = create_flow_run(
                    flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                    project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                    parameters={
                        "project_id": "datario",
                        "dataset_id": dataset_id,
                        "table_id": table_id,
                        "maximum_bytes_processed": maximum_bytes_processed,
                    },
                    labels=[
                        "datario",
                    ],
                    run_name=f"Dump to GCS {dataset_id}.{table_id}",
                )
                dump_to_gcs_flow.set_upstream(wait_for_materialization)

                wait_for_dump_to_gcs = wait_for_flow_run(
                    dump_to_gcs_flow,
                    stream_states=True,
                    stream_logs=True,
                    raise_final_state=True,
                )


daily_geolocator_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
daily_geolocator_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
daily_geolocator_flow.schedule = every_day_at_four_am
