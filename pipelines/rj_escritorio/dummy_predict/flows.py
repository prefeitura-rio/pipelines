# -*- coding: utf-8 -*-
"""
Predict flow example usage.
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.rj_escritorio.dummy_predict.tasks import (
    get_current_timestamp,
    get_dummy_input_data,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.predict_flow.tasks import (
    prepare_dataframe_for_prediction,
)
from pipelines.utils.tasks import get_current_flow_labels

with Flow(
    name="EMD: escritorio - Exemplo de flow para predição com modelo",
    code_owners=[
        "gabriel",
    ],
) as dummy_predict_example_flow:

    ###########################
    #     Your parameters     #
    ###########################
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    dump_mode = Parameter("dump_mode", default="append", required=False)

    ###########################
    #  Prediction parameters  #
    ###########################
    # You can check available parameters in the prediction flow definition
    # pipelines/utils/predict_flow/flows.py
    tracking_server_uri = Parameter("tracking_server_uri", default=None, required=False)
    model_name = Parameter("model_name")
    model_version_or_stage = Parameter(
        "model_version_or_stage", default="Production", required=False
    )

    # Fetch any data you want to use for prediction (pd.DataFrame)
    # It must match the same structure as the data you've used for
    # training the model
    input_data = get_dummy_input_data()
    flow_start_timestamp = get_current_timestamp()

    # Prepare dataframe for prediction (this is just a way of wrapping
    # the dataframe to JSON format, but it's mandatory)
    data_for_prediction = prepare_dataframe_for_prediction(
        dataframe=input_data,
    )

    # Call the prediction flow
    current_flow_labels = get_current_flow_labels()
    # Trigger DBT flow run
    prediction_flow = create_flow_run(
        flow_name=utils_constants.FLOW_PREDICT_NAME.value,
        project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        parameters={
            "tracking_server_uri": tracking_server_uri,
            "model_name": model_name,
            "model_version_or_stage": model_version_or_stage,
            "input_data": data_for_prediction,
            "output_type": "bigquery",
            "dataset_id": dataset_id,
            "table_id": table_id,
            "dump_mode": dump_mode,
            "output_column_name": "prediction",
            "save_dataframe_path": "/tmp/predictions.csv",
            "include_timestamp": True,
            "timestamp": flow_start_timestamp,
        },
        labels=current_flow_labels,
        run_name=f"Predict {dataset_id}.{table_id}",
    )

    wait_for_materialization = wait_for_flow_run(
        prediction_flow,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )


dummy_predict_example_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dummy_predict_example_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
