# -*- coding: utf-8 -*-
"""
Flows for projeto_subsidio_sppo
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

import pendulum
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.schedules import every_day
from pipelines.rj_smtr.tasks import (
    fetch_dataset_sha,
    query_table_and_save_local,
    run_dbt_model,
    upload_to_storage,
)

# from pipelines.rj_smtr.projeto_subsidio_sppo.schedules import every_two_weeks
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client


with Flow(
    "SMTR - Subsidio - Salvar no Storage", code_owners=["caio", "fernanda"]
) as subsidio_dump_gcs:
    project_id = Parameter("project_id", default=None)
    dataset_id = Parameter("dataset_id", default="projeto_subsidio_sppo")
    table_id = Parameter("table_id", default="viagem_completa")
    date_range = Parameter("date_range", default=None)
    if_exists = Parameter("if_exists", default="replace")

    filepath = query_table_and_save_local(
        dataset_id=dataset_id,
        table_id=table_id,
        project_id=project_id,
        date_range=date_range,
    )
    upload_to_storage(
        dataset_id=dataset_id, table_id=table_id, filepath=filepath, if_exists=if_exists
    )

with Flow(
    "SMTR - Subsidio SPPO - Materialização",
    code_owners=["caio", "fernanda"],
) as subsidio_materialize_flow:
    run_date = Parameter(
        "run_date", default={"run_date": pendulum.now(constants.TIMEZONE.value).date()}
    )
    dataset_id = Parameter(
        "dataset_id", default=constants.SUBSIDIO_SPPO_DATASET_ID.value
    )
    table_id = Parameter("table_id", default=constants.SUBSIDIO_SPPO_TABLE_ID.value)

    refresh = Parameter("refresh", default=False)

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode="prod")
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)
    dataset_sha = fetch_dataset_sha(
        dataset_id=dataset_id,
    )
    # Run materialization #
    with case(refresh, True):
        FIRST = run_dbt_model(
            dbt_client=dbt_client,
            model=table_id,
            upstream=True,
            _vars=[run_date, dataset_sha],
            flags="--full-refresh",
        )
    with case(refresh, False):
        FIRST = run_dbt_model(
            dbt_client=dbt_client,
            model=table_id,
            _vars=[run_date, dataset_sha],
            upstream=True,
        )

subsidio_materialize_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
subsidio_materialize_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value
)
subsidio_materialize_flow.schedule = every_day

subsidio_dump_gcs.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
subsidio_dump_gcs.run_config = KubernetesRun(image=emd_constants.DOCKER_IMAGE.value)
