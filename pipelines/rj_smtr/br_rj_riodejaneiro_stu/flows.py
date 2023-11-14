# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_stu
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefect import Parameter, case, task
from prefect.tasks.control_flow import merge


# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_current_flow_labels,
)


from pipelines.utils.utils import set_default_parameters

# SMTR Imports #

from pipelines.rj_smtr.flows import (
    default_capture_flow,
)

from pipelines.rj_smtr.tasks import get_rounded_timestamp, get_current_timestamp

from pipelines.rj_smtr.constants import constants
