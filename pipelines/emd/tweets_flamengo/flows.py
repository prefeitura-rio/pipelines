"""
Flows for emd
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
#   configurar o storage como o pipelines.emd
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

from functools import partial

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.emd.tweets_flamengo.tasks import (
    fetch_last_id,
    get_last_id,
    fetch_tweets,
    save_last_id,
    upload_to_storage,
    get_api,
)
from pipelines.utils import notify_discord_on_failure

# from pipelines.emd.tweets_flamengo.schedules import tweets_flamengo_schedule


with Flow(
    name="EMD: escritorio - Captura tweets do Flamengo",
    on_failure=partial(notify_discord_on_failure,
                       secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value),
) as tweets_flamengo_flow:
    q = Parameter("keyword")

    api = get_api()

    data_path = fetch_last_id(q=q)  # pylint: disable=C0103

    last_id, created_at = get_last_id(api=api, q=q, data_path=data_path)

    dd = fetch_tweets(api=api, q=q, last_id=last_id, created_at=created_at)

    path = save_last_id(df=dd, q=q)  # pylint: disable=C0103

    upload_to_storage(path)

tweets_flamengo_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
tweets_flamengo_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value)
# tweets_flamengo_flow.schedule = tweets_flamengo_schedule
