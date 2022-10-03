# -*- coding: utf-8 -*-
"""
Flows for INEA.
"""
from prefect import Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.inea.tasks import convert_vol_files, fetch_vol_files
from pipelines.utils.decorators import Flow

with Flow(
    "INEA: Teste",
    code_owners=[
        "gabriel",
    ],
) as inea_test_flow:
    date = Parameter("date")
    FETCH_TASK = fetch_vol_files(date=date)
    CONVERT_TASK = convert_vol_files()
    CONVERT_TASK.set_upstream(FETCH_TASK)

inea_test_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_test_flow.run_config = LocalRun(labels=[constants.INEA_AGENT_LABEL.value])
