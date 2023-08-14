# -*- coding: utf-8 -*-
"test_error flows"
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS


from pipelines.utils.decorators import Flow
from pipelines.constants import constants as emd_constants

from pipelines.rj_smtr.test_errors.tasks import test_value_error
from pipelines.rj_smtr.test_errors.schedules import (
    every_minute,
    every_10_minutes,
    every_hour,
    every_day,
)

with Flow(
    "SMTR - Test Errors for Notification", code_owners=["boris", "caio"]
) as test_error_flow:
    test_value_error()

test_error_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
test_error_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
test_error_flow.schedule = every_minute

test_error_10_minutes = deepcopy(test_error_flow)
test_error_10_minutes.schedule = every_10_minutes

test_error_hourly = deepcopy(test_error_flow)
test_error_hourly.schedule = every_hour

test_error_daily = deepcopy(test_error_flow)
test_error_daily.schedule = every_day
