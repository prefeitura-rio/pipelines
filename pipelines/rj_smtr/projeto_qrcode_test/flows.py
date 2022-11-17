# -*- coding: utf-8 -*-
"""
Flows for projeto_qrcode_test
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

# EMD Import # => Entidades gerais das secretarias (padrão)
from pipelines.constants import constants  # constantes de todas as secretarias
from pipelines.utils.decorators import Flow

# SMTR Import # => Entidades da SMTR (tasks, constants, etc)
from pipelines.rj_smtr.projeto_qrcode_test.tasks import filter_gps_data
from pipelines.rj_smtr.schedules import every_minute_dev
from pipelines.rj_smtr.constants import (
    constants as smtr_constants,
)  # constantes da SMTR (podem ser adicionadas novas)

# Tasks de outras pipelines podem ser importadas
from pipelines.rj_smtr.br_rj_riodejaneiro_brt_gps.tasks import create_api_url_brt_gps
from pipelines.rj_smtr.tasks import get_raw

with Flow(
    "my_flow",
    code_owners=[
        "@your-discord-username",
    ],  # ignorar
) as rj_smtr_projeto_qrcode_test_flow:

    # ==> Como estruturar a pipeline?

    # 1. Definir parâmetros de entrada que serão passados as Flow
    id_veiculo = Parameter("id_veiculo", default=smtr_constants.DEFAULT_VEHICLE.value)
    # ...)

    # 2. Definir as tasks (funções) que serão executadas

    # 2.1 Ex: Puxar dados de GPS - temos já funções padrão para isso
    url = create_api_url_brt_gps()
    raw_status = get_raw(
        url=url
    )  # retorna dicionário com dados + erro, caso haja um erro

    # 2.2 Podem ser criadas tasks customizadas, como por exemplo:
    filter_status = filter_gps_data(raw_status=raw_status, id_veiculo=id_veiculo)

    # 2.3 Ex: Salvar capturar e salvar dados no Storage: salvamos sempre os dados
    # organizados em partições Hive (no geral, partitionados em
    # data/hora de captura) - ver exemplo completo no flow de
    # captura do BRT (linha 138 a 158 em rj_smtr/br_rj_riodejaneiro_brt_gps)

# 3. Definir as configurações para execução do flow
rj_smtr_projeto_qrcode_test_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)  # (padrão)
rj_smtr_projeto_qrcode_test_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,  # (padrão)
    labels=[
        constants.RJ_SMTR_DEV_AGENT_LABEL.value
    ],  # Agent que vai executar o flow (nesse caso, dev)
)
# rj_smtr_projeto_qrcode_test_flow.schedule = every_minute_dev # rotina
# de execução do flow (nesse caso, a cada minuto em dev)
