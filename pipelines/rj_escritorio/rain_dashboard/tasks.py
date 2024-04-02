# -*- coding: utf-8 -*-
"""
Tasks for setting rain data in Redis.
"""
from typing import Dict, List, Union

import basedosdados as bd
from basedosdados.upload.base import Base
import pandas as pd
from prefect import task

from pipelines.utils.utils import get_redis_client, log


@task(checkpoint=False)
def get_data(query: str, mode: str = "prod") -> pd.DataFrame:
    """
    Load rain data from BigQuery
    """

    # Get billing project ID
    log("Inferring billing project ID from environment.")
    billing_project_id: str = None
    try:
        bd_base = Base()
        billing_project_id = bd_base.config["gcloud-projects"][mode]["name"]
    except KeyError:
        pass
    if not billing_project_id:
        raise ValueError(
            "billing_project_id must be either provided or inferred from environment variables"
        )
    log(f"Billing project ID: {billing_project_id}")

    # Load data
    log("Loading data from BigQuery...")
    dataframe = bd.read_sql(
        query=query, billing_project_id=billing_project_id, from_file=True
    )

    # Type assertions
    if "chuva_15min" in dataframe.columns:
        dataframe["chuva_15min"] = dataframe["chuva_15min"].astype("float64")
    else:
        log(
            'Column "chuva_15min" not found in dataframe, skipping type assertion.',
            "warning",
        )

    log("Data loaded successfully.")

    return dataframe


@task(checkpoint=False)
def dataframe_to_dict(dataframe: pd.DataFrame) -> List[Dict[str, Union[str, float]]]:
    """
    Convert dataframe to dictionary
    """
    log("Converting dataframe to dictionary...")
    return dataframe.to_dict(orient="records")


@task(checkpoint=False)
def set_redis_key(
    key: str,
    value: List[Dict[str, Union[str, float]]],
    host: str = "redis.redis.svc.cluster.local",
    port: int = 6379,
    db: int = 0,  # pylint: disable=C0103
) -> None:
    """
    Set Redis key
    """
    log("Setting Redis key...")
    redis_client = get_redis_client(host=host, port=port, db=db)
    redis_client.set(key, value)
    log("Redis key set successfully.")
    log(f"key: {key} and value: {value}")
