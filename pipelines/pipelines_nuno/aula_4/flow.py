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
    plota_idades
)

from pipelines.constants import constants
from pipelines.utils.decorators import Flow

with Flow(
    "SME:Exercicio esquenta - ler API de dados pessoais",
    code_owners=[
      "nunocaminada",
    ],
) as aula_4_flow:

    timestampArquivo = get_time_stamp()
    my_caminho = get_caminho_saida()

    numero_usuarios = Parameter("numero_usuarios", default=10)
    dataframeOriginal = get_data(numero_usuarios)

    #formata os numeros de celular e de telefone fixo
    DataframeTratada = transforma_celulares(dataframeOriginal,'cell')
    DataframeTratada = transforma_celulares(dataframeOriginal,'phone')

    #Grava em arquivo CSV
    escreve_resultado(DataframeTratada, timestampArquivo)
    plota_idades(DataframeTratada,timestampArquivo)

    #Imprime os calculos de porcentagem
    print(grava_estatisticas(DataframeTratada, timestampArquivo))

aula_4_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
aula_4_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SME_AGENT_LABEL.value],
)
aula_4_flow.schedule = None  
 