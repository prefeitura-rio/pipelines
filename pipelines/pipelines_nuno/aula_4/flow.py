# -*- coding: utf-8 -*-
"""
Flows para o exercicio de esquenta (SME)
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.pipelines_nuno.aula_4.tasks import (
    getTimestamp,
    getCaminhoSaida,
    getData,
    escreveResultado,
    transformaCelulares,
    gravaEstatisticas,
    plotaIdades
)

from pipelines.constants import constants
from pipelines.utils.decorators import Flow

with Flow(
    "SME:Exercicio esquenta - ler API de dados pessoais",
    code_owners=[
      "nunocaminada",
    ],
) as aula_4_flow:

    timestampArquivo = getTimestamp()
    myCaminho = getCaminhoSaida()

    numero_usuarios = Parameter("numero_usuarios", default=10)
    dataframeOriginal = getData(numero_usuarios)

    #formata os numeros de celular e de telefone fixo
    DataframeTratada = transformaCelulares(dataframeOriginal,'cell')
    DataframeTratada = transformaCelulares(dataframeOriginal,'phone')

    #Grava em arquivo CSV
    escreveResultado(DataframeTratada, timestampArquivo)
    plotaIdades(DataframeTratada,timestampArquivo)

    #Imprime os calculos de porcentagem
    print(gravaEstatisticas(DataframeTratada, timestampArquivo))

aula_4_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
aula_4_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SME_AGENT_LABEL.value],
)
aula_4_flow.schedule = None  
 