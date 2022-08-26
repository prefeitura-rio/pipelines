# -*- coding: utf-8 -*-
"""
Predict flow definition.
"""

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.predict_flow.tasks import (
    generate_dataframe_from_predictions,
    get_model,
    predict,
)
from pipelines.utils.tasks import create_table_and_upload_to_gcs

with Flow(
    name=utils_constants.FLOW_PREDICT_NAME.value,
    code_owners=[
        "gabriel",
    ],
) as predict_with_mlflow_model_flow:

    # MLflow parameters
    tracking_server_uri = Parameter("tracking_server_uri", default=None, required=False)

    # Model parameters
    model_name = Parameter("model_name")
    model_version_or_stage = Parameter(
        "model_version_or_stage", default="Production", required=False
    )

    # Input parameters
    input_data = Parameter("input_data")

    # Output parameters
    output_type = Parameter("output_type", default="bigquery", required=False)
    dataset_id = Parameter("dataset_id", default=None, required=False)
    table_id = Parameter("table_id", default=None, required=False)
    dump_mode = Parameter("dump_mode", default="append", required=False)
    output_column_name = Parameter("output_column_name", default=None, required=False)
    save_dataframe_path = Parameter(
        "save_dataframe_path", default="/tmp/predictions.csv", required=False
    )

    # Get model from MLflow model registry (either model version or stage must be provided)
    model = get_model(
        model_name=model_name,
        model_version_or_stage=model_version_or_stage,
        tracking_server_uri=tracking_server_uri,
    )

    # Make predictions using model
    predictions = predict(
        data=input_data,
        model=model,
    )

    # Generate dataframe from predictions and save it to a temporary path
    result_df = generate_dataframe_from_predictions(
        predictions=predictions,
        output_column_name=output_column_name,
        save_path=save_dataframe_path,
    )

    # If the output_type is `bigquery`, create a table and upload the dataframe to it
    with case(output_type, "bigquery"):
        create_table_and_upload_to_gcs(
            data_path=save_dataframe_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
        )

predict_with_mlflow_model_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
predict_with_mlflow_model_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
