# -*- coding: utf-8 -*-
"""
flows for Policy Matrix
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_escritorio.dump_policy_matrix.schedules import (
    every_week,
    project_ids,
)
from pipelines.utils.policy_matrix.flows import utils_policy_matrix_flow
from pipelines.utils.utils import set_default_parameters

policy_matrix_flow = deepcopy(utils_policy_matrix_flow)
policy_matrix_flow.name = "EMD: Policy Matrix - Dump Permissoes"
policy_matrix_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
policy_matrix_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
)

policy_matrix_default_parameters = {"project_ids": project_ids, "mode": "dev"}
policy_matrix_flow = set_default_parameters(
    policy_matrix_flow, default_parameters=policy_matrix_default_parameters
)

policy_matrix_flow.schedule = every_week
