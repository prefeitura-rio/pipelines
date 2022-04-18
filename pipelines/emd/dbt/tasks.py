"""
Tasks related to DBT flows.
"""

from dbt_client import DbtClient
from prefect import task

from pipelines.emd.dbt.utils import (
    get_dbt_client,
)


@task(checkpoint=False)
def get_k8s_dbt_client(
    mode: str = "dev",
) -> DbtClient:
    """
    Get a DBT client for the Kubernetes cluster.
    """
    if mode not in ["dev", "prod"]:
        raise ValueError(f"Invalid mode: {mode}")
    return get_dbt_client(
        host=f"dbt-rpc-{mode}",
    )


@task(checkpoint=False)
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
