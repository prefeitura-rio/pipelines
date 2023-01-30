# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""
Flow for generating policy matrix
"""

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.georeference.tasks import (
    dataframe_to_csv,
    georeference_dataframe,
    get_new_addresses,
    validate_georeference_mode,
)
from pipelines.utils.tasks import (
    get_current_flow_labels,
    create_table_and_upload_to_gcs,
)

with Flow(
    "EMD: utils - Gerar matriz de políticas de acesso",
    code_owners=[
        "gabriel",
        "diego",
    ],
) as utils_policy_matrix_flow:

    # Parameters

