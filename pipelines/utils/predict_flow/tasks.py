# -*- coding: utf-8 -*-
"""
Tasks for the predict flow.
"""

from typing import Any, List

import pandas as pd
from prefect import task
import requests

from pipelines.utils.utils import log


@task(checkpoint=False)
def predict(df: pd.DataFrame, model_serving_url: str) -> List[Any]:
    """
    Uses an MLflow model to predict using the data in the dataframe.

    This must be similar to the curl command:

    ```
    curl -X POST -H "Content-Type:application/json; format=pandas-split" \
        --data '{"columns":["col1", "col2", ...],"data":[[0.1, 0.2, ...]]}' \
        https://url-for-model/invocations
    ```
    """
    data = df.to_json(orient="split")
    headers = {"Content-Type": "application/json; format=pandas-split"}
    response = requests.post(model_serving_url, data=data, headers=headers)
    log(f"Sending data to model serving URL: {model_serving_url}")
    data = response.json()
    log(f"Response from model serving URL: {data}")
    return data
