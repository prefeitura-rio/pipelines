# -*- coding: utf-8 -*-
"""
Tasks for the predict flow.
"""

from pathlib import Path
from typing import Any, Dict, List, Union

import mlflow
import mlflow.pyfunc
from numpy import ndarray
import pandas as pd
import pendulum
from prefect import task

from pipelines.constants import constants
from pipelines.utils.utils import dataframe_to_csv,log


@task(checkpoint=False)
def get_model(
    model_name: str,
    model_version_or_stage: str,
    tracking_server_uri: str = None,
):
    """
    Get model from MLflow model registry.
    """
    if not tracking_server_uri:
        tracking_server_uri = constants.MLFLOW_TRACKING_URI.value
    mlflow.set_tracking_uri(tracking_server_uri)
    model_uri = f"models:/{model_name}/{model_version_or_stage}"
    log(f"Tracking server URI: {tracking_server_uri}")
    log(f"Model URI: {model_uri}")
    model = mlflow.pyfunc.load_model(model_uri=model_uri)
    return model


@task(checkpoint=False)
def predict(data: Dict[str, List[Any]], model: mlflow.pyfunc.PyFuncModel) -> ndarray:
    """
    Uses an MLflow model to predict using the data in the dataframe.
    """
    # From pandas-split to dataframe
    dataframe = pd.DataFrame(data=data["data"], columns=data["columns"])
    # Predict
    predictions = model.predict(dataframe)
    return list(predictions)


@task(checkpoint=False)
def generate_dataframe_from_predictions(
    predictions: List[Any],
    output_column_name: str,
    include_timestamp: bool = False,
    timestamp: str = None,
    save_path: Union[str, Path] = None,
) -> pd.DataFrame:
    """
    Generate a dataframe from the predictions.
    """
    dataframe = pd.DataFrame(data=predictions, columns=[output_column_name])
    if include_timestamp:
        if not timestamp:
            timestamp = pendulum.now(
                tz=constants.DEFAULT_TIMEZONE.value
            ).to_datetime_string()
        dataframe["timestamp"] = timestamp
    if save_path:
        if not isinstance(save_path, Path):
            save_path = Path(save_path)
        # save_path.mkdir(parents=True, exist_ok=True)
        # dataframe.to_csv(save_path / "data.csv", index=False)
        dataframe_to_csv(dataframe=dataframe, path = save_path / "data.csv")
    return dataframe


@task(checkpoint=False)
def prepare_dataframe_for_prediction(dataframe: pd.DataFrame) -> Dict[str, List[Any]]:
    """
    Use pandas split
    """
    return dataframe.to_dict(orient="split")
