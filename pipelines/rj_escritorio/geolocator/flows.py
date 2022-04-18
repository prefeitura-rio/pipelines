# -*- coding: utf-8 -*-
# pylint: disable="unexpected-keyword-arg", C0103

"""
Flows for geolocator
"""


from prefect import Flow, Parameter, case
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
    enderecos_novos,
    geolocaliza_enderecos,
    importa_bases_e_chamados,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.tasks import (
    get_current_flow_labels,
    create_table_and_upload_to_gcs,
)

with Flow("EMD: escritorio - Geolocalizacao de chamados 1746") as daily_geolocator_flow:
    # [enderecos_conhecidos, enderecos_ontem, chamados_ontem, base_enderecos_atual]
    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_to_datario = Parameter("materialize_to_datario", default=False, required=False)
    dataset_id = geolocator_constants.DATASET_ID.value
    table_id = geolocator_constants.TABLE_ID.value

    lista_enderecos = importa_bases_e_chamados()
    novos_enderecos = enderecos_novos(
        lista_enderecos=lista_enderecos, upstream_tasks=[lista_enderecos]
    )
    base_geolocalizada = geolocaliza_enderecos(
        base_enderecos_novos=novos_enderecos, upstream_tasks=[novos_enderecos]
    )
    csv_criado = cria_csv(  # pylint: disable=invalid-name
        base_enderecos_atual=lista_enderecos[3],
        base_enderecos_novos=base_geolocalizada,
        upstream_tasks=[base_geolocalizada],
    )
    # today = get_today()
    create_table_and_upload_to_gcs(
        data_path=geolocator_constants.PATH_BASE_ENDERECOS.value,
        dataset_id=geolocator_constants.DATASET_ID.value,
        table_id=geolocator_constants.TABLE_ID.value,
        dump_type="append",
        upstream_tasks=[csv_criado],
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

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
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
