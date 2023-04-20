# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.formacao_joao.tasks import (
    hello_name,
)
from pipelines.utils.decorators import Flow

with Flow(
    "EMD: formacao - Exemplo para formação em infraestrutura",
    code_owners=[
        "gabriel",
        "diego",
        "joao",
    ],
) as rj_escritorio_formacao_joao_flow:
    name = Parameter("name", default="Infra_Joao")
    hello_name(name=name)


rj_escritorio_formacao_joao_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio_formacao_joao_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
