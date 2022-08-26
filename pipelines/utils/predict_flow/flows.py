# -*- coding: utf-8 -*-
"""
Predict flow definition.
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow

with Flow(
    name=utils_constants.FLOW_PREDICT_NAME.value,
    code_owners=[
        "gabriel",
    ],
) as predict_with_mlflow_model_flow:

    # Model parameters
    model_name = Parameter("model_name")
    model_version = Parameter("model_version", default=None, required=False)
    model_stage = Parameter("model_stage", default=None, required=False)

    # Input parameters
    input_data = Parameter("input_data")

    # Output parameters
    output_type = Parameter("output_type", default="bigquery", required=False)
    dataset_id = Parameter("dataset_id", default=None, required=False)
    table_id = Parameter("table_id", default=None, required=False)
    columns = Parameter("columns", default=None, required=False)

    # TODO: get model from MLflow model registry (either model version or stage must be provided)

    # TODO: predict with model

    # TODO: save output
