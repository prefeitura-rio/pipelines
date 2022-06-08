# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_sigmob
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
#   configurar o storage como o pipelines.rj_smtr
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

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.br_rj_riodejaneiro_sigmob.constants import (
    constants as sigmob_constants,
)
from pipelines.rj_smtr.tasks import (
    bq_upload_from_dict,
    run_dbt_command,
    build_incremental_model,
    # , get_local_dbt_client
)
from pipelines.rj_smtr.br_rj_riodejaneiro_sigmob.schedules import every_day
from pipelines.rj_smtr.br_rj_riodejaneiro_sigmob.tasks import request_data

from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import (
    get_k8s_dbt_client,
)

with Flow(
    "SMTR - DBT execute - SIGMOB",
    code_owners=["@hellcassius#1223", "@fernandascovino#9750"],
) as materialize_sigmob:
    dataset_id = Parameter("dataset_id", default="br_rj_riodejaneiro_sigmob")
    backfill = Parameter("backfill", default=False)

    dbt_client = get_k8s_dbt_client(mode="prod")
    # For local development: comment above and uncomment below
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)
    with case(backfill, True):
        RUN = run_dbt_command(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            flags="--full-refresh",
        )
        INCREMENTAL_RUN = build_incremental_model(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            base_table_id="shapes",
            mat_table_id="shapes_geom",
            wait=RUN,
        )
        LAST_RUN = run_dbt_command(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            table_id="data_versao_efetiva",
            flags="--full-refresh",
            wait=INCREMENTAL_RUN,
        )
        TESTS = run_dbt_command(
            command="test", dbt_client=dbt_client, dataset_id=dataset_id, wait=LAST_RUN
        )
    with case(backfill, False):

        RUN = run_dbt_command(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
        )

        TESTS = run_dbt_command(
            command="test", dbt_client=dbt_client, dataset_id=dataset_id, wait=RUN
        )
    materialize_sigmob.set_dependencies(
        task=LAST_RUN, upstream_tasks=[dbt_client, RUN, INCREMENTAL_RUN]
    )

with Flow(
    "SMTR- Captura - SIGMOB",
    code_owners=[
        "@your-discord-username",
    ],
) as captura_sigmob:

    endpoints = Parameter("endpoints", default=sigmob_constants.ENDPOINTS.value)
    dataset_id = Parameter("dataset_id", default="br_rj_riodejaneiro_sigmob")
    materialize = Parameter("materialize", default=True)

    paths_dict = request_data(endpoints=endpoints)
    bq_upload = bq_upload_from_dict(paths=paths_dict, dataset_id=dataset_id)
    with case(materialize, True):
        materialize_run = create_flow_run(
            flow_name=materialize_sigmob.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={"dataset_id": "br_rj_riodejaneiro_sigmob", "backfill": False},
            labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
            run_name="SMTR - Materialzar SIGMOB após captura",
        )
        materialize_run.set_upstream(bq_upload)
    wait_for_materialization = wait_for_flow_run(
        materialize_run,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

materialize_sigmob.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
materialize_sigmob.run_config = KubernetesRun(image=emd_constants.DOCKER_IMAGE.value)
# materialize_sigmob.schedule = every_day

captura_sigmob.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_sigmob.run_config = KubernetesRun(image=emd_constants.DOCKER_IMAGE.value)
captura_sigmob.schedule = every_day
