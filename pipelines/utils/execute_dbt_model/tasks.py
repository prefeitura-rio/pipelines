# -*- coding: utf-8 -*-
"""
Tasks related to DBT flows.
"""
# pylint: disable=unused-argument

from datetime import timedelta

from dbt_client import DbtClient
from prefect import task

from pipelines.utils.execute_dbt_model.utils import (
    get_dbt_client,
)
from pipelines.constants import constants


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_k8s_dbt_client(
    mode: str = "dev",
    wait=None,
) -> DbtClient:
    """
    Get a DBT client for the Kubernetes cluster.
    """
    if mode not in ["dev", "prod"]:
        raise ValueError(f"Invalid mode: {mode}")
    return get_dbt_client(
        host=f"dbt-rpc-{mode}",
    )


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def run_dbt_model(
    dbt_client: DbtClient,
    dataset_id: str,
    table_id: str,
    sync: bool = True,
):
    """
    Run a DBT model.
    """
    dbt_client.cli(
        f"run --models {dataset_id}.{table_id}",
        sync=sync,
    )
