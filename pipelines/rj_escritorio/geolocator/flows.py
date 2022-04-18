# -*- coding: utf-8 -*-
# pylint: disable="unexpected-keyword-arg", C0103

"""
Flows for geolocator
"""

###############################################################################
#
# Aqui é onde devem ser definidos os flows do projeto.
# Cada flow representa uma sequência de passos que serão executados
# em ordem.
#
# Mais informações sobre flows podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/flows.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# Existem diversas maneiras de declarar flows. No entanto, a maneira mais
# conveniente e recomendada pela documentação é usar a API funcional.
# Em essência, isso implica simplesmente na chamada de funções, passando
# os parâmetros necessários para a execução em cada uma delas.
#
# Também, após a definição de um flow, para o adequado funcionamento, é
# mandatório configurar alguns parâmetros dele, os quais são:
# - storage: onde esse flow está armazenado. No caso, o storage é o
#   próprio módulo Python que contém o flow. Sendo assim, deve-se
#   configurar o storage como o pipelines.rj_escritorio
# - run_config: para o caso de execução em cluster Kubernetes, que é
#   provavelmente o caso, é necessário configurar o run_config com a
#   imagem Docker que será usada para executar o flow. Assim sendo,
#   basta usar constants.DOCKER_IMAGE.value, que é automaticamente
#   gerado.
# - schedule (opcional): para o caso de execução em intervalos regulares,
#   deve-se utilizar algum dos schedules definidos em schedules.py
#
# Um exemplo de flow, considerando todos os pontos acima, é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
# from prefect import Flow
# from prefect.run_configs import KubernetesRun
# from prefect.storage import GCS
# from pipelines.constants import constants
# from my_tasks import my_task, another_task
# from my_schedules import some_schedule
#
# with Flow("my_flow") as flow:
#     a = my_task(param1=1, param2=2)
#     b = another_task(a, param3=3)
#
# flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = some_schedule
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################


from prefect import Flow, Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.rj_escritorio.geolocator.constants import (
    constants as geolocator_constants,
)
from pipelines.rj_escritorio.geolocator.schedules import every_day_at_four_am
from pipelines.rj_escritorio.geolocator.tasks import (
    cria_csv,
    enderecos_novos,
    geolocaliza_enderecos,
    importa_bases_e_chamados,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.tasks import upload_to_gcs, get_current_flow_labels

with Flow("EMD: escritorio - Geolocalizacao de chamados 1746") as daily_geolocator_flow:
    # [enderecos_conhecidos, enderecos_ontem, chamados_ontem, base_enderecos_atual]
    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
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
    upload_to_gcs(
        path=geolocator_constants.PATH_BASE_ENDERECOS.value,
        dataset_id=dataset_id,
        table_id=table_id,
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
daily_geolocator_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
daily_geolocator_flow.schedule = every_day_at_four_am
