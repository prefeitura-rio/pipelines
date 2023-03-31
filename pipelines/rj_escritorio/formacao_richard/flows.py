# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.formacao_richard.tasks import (
    add_numbers,
)
from pipelines.utils.decorators import Flow

with Flow(
    "EMD: formação - Richard",
    code_owners=[
        "gabriel",
    ],
    skip_if_running=True,
) as rj_escritorio_formacao_richard_example_flow:
    number1 = Parameter("number1", default=1)
    number2 = Parameter("number2", default=2)
    add_numbers(number1=number1, number2=number2)

rj_escritorio_formacao_richard_example_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
rj_escritorio_formacao_richard_example_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
