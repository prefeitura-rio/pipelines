# -*- coding: utf-8 -*-
"""
Flows para o exercicio de esquenta (SME)
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.pipelines_nuno.aula_4.tasks import (
    get_time_stamp,
    get_caminho_saida,
    get_data,
    escreve_resultado,
    transforma_celulares,
    grava_estatisticas,
    plota_idades,
)

from pipelines.constants import constants
from pipelines.utils.decorators import Flow

with Flow(
    "SME:Exercicio esquenta - ler API de dados pessoais",
    code_owners=[
        "nunocaminada",
    ],
) as aula_4_flow:

    timestamp_arquivo = get_time_stamp()
    my_caminho = get_caminho_saida()

    numero_usuarios = Parameter("numero_usuarios", default=10)
    dataframe_original = get_data(numero_usuarios)

    # formata os numeros de celular e de telefone fixo
    dataframe_tratada = transforma_celulares(dataframe_original, "cell")
    dataframe_tratada = transforma_celulares(dataframe_original, "phone")

    # Grava em arquivo CSV
    escreve_resultado(dataframe_tratada, timestamp_arquivo)
    plota_idades(dataframe_tratada, timestamp_arquivo)

    # Imprime os calculos de porcentagem
    print(grava_estatisticas(dataframe_tratada, timestamp_arquivo))

aula_4_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
aula_4_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SME_AGENT_LABEL.value],
)
aula_4_flow.schedule = None
