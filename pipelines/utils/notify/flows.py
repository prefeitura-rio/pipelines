# -*- coding: utf-8 -*-
"""
Flows for notify
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
#   configurar o storage como o pipelines.utils
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

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_escritorio.cleanup.tasks import get_prefect_client
from pipelines.utils.notify.tasks import (
    format_failed_runs,
    post_to_discord_from_secret,
    query_flow_runs_by_state,
)
from pipelines.utils.tasks import get_bool

from pipelines.utils.notify.schedules import smtr_every_fifteen_minutes
from pipelines.utils.decorators import Flow

with Flow(
    "my_flow",
    code_owners=[
        "@your-discord-username",
    ],
) as discord_batch_notify:
    # SETUP
    prefix = Parameter("prefix", default=None)
    query_interval = Parameter("query_interval", default=dict(hours=1))
    datetime_filter = Parameter("datetime_filter", default=None)
    webhook_secret_path = Parameter("webhook_secret_path", default=None)

    client = get_prefect_client()
    response = query_flow_runs_by_state(
        _client=client,
        prefix=prefix,
        interval=query_interval,
        datetime_filter=datetime_filter,
    )
    with case(get_bool(response), True):
        message = format_failed_runs(response)
        post_to_discord_from_secret(
            message=message, webhook_secret_path=webhook_secret_path
        )

discord_batch_notify.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
discord_batch_notify.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
discord_batch_notify.schedule = smtr_every_fifteen_minutes
