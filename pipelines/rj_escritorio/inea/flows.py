# -*- coding: utf-8 -*-
"""
Flows for INEA.
"""
from prefect.run_configs import DockerRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.inea.tasks import print_environment_variables
from pipelines.utils.decorators import Flow

with Flow(
    "INEA: Teste",
    code_owners=[
        "gabriel",
    ],
) as inea_test_flow:
    print_environment_variables()

inea_test_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_test_flow.run_config = DockerRun(
    image=constants.DOCKER_IMAGE.value, labels=[constants.INEA_AGENT_LABEL.value]
)
