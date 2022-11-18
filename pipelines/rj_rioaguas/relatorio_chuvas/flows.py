# -*- coding: utf-8 -*-
"""
Flows para pipeline de dados de nível da Lagoa Rodrigo de Freitas.
Fonte: Squitter.
"""
# pylint: disable=C0327

from prefect import Parameter

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.rj_rioaguas.relatorio_chuvas.tasks import download_file, salvar_dados

with Flow(
    "RIOAGUAS: Relatorio de Chuvas - Nivel LRF",
    code_owners=["JP"],
) as rioaguas_nivel_LRF:
    # Parâmetros
    download_url = Parameter(
        "download_url", default="http://horus.squitter.com.br/dados/meteorologicos/292/"
    )

    # Tasks
    dados = download_file(download_url)
    salvar_dados(dados)
  
rioaguas_nivel_LRF.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rioaguas_nivel_LRF.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_RIOAGUAS_AGENT_LABEL.value,
    ],
)
rioaguas_nivel_LRF.schedule = None
