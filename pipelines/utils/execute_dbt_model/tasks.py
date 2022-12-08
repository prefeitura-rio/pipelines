# -*- coding: utf-8 -*-
"""
Tasks related to DBT flows.
"""
# pylint: disable=unused-argument, R0914

from datetime import timedelta
from typing import Any, Dict, List, Union

from dbt_client import DbtClient
from prefect import task

from pipelines.utils.execute_dbt_model.utils import (
    get_dbt_client,
    parse_dbt_logs,
)
from pipelines.constants import constants
from pipelines.utils.utils import get_vault_secret, log


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
# pylint: disable=too-many-arguments
def run_dbt_model(
    dbt_client: DbtClient,
    dataset_id: str = None,
    table_id: str = None,
    model: str = None,
    dbt_alias: bool = False,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: Union[dict, List[Dict]] = None,
    sync: bool = True,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Run a DBT model.
    """
    run_command = "dbt run"
    if dbt_alias:
        table_id = f"{dataset_id}__{table_id}"

    if not model:
        model = f"{dataset_id}"
        if table_id:
            model += f".{table_id}"

    # Set models and upstream/downstream for dbt
    if model:
        run_command += " --select "
        if upstream:
            run_command += "+"
        run_command += f"{model}"
        if downstream:
            run_command += "+"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"
    if flags:
        run_command += f" {flags}"

    log(f"Will run the following command:\n{run_command}")
    logs_dict = dbt_client.cli(
        run_command,
        sync=sync,
        logs=True,
    )
    parse_dbt_logs(logs_dict, log_queries=True)
    return log("Finished running dbt model")


@task(checkpoint=False)
def is_running_at_datario(current_flow_labels: List[str]) -> bool:
    """
    Check if the current flow is running at datario agent.
    """
    return "datario" in current_flow_labels


@task
def is_valid_dictionary(dictionary: Dict[Any, Any]) -> Dict[str, Any]:
    """
    Checks whether the dictionary is in the format Dict[str, str].
    """
    if not isinstance(dictionary, dict):
        raise ValueError("The dictionary is not a dictionary.")
    if not all(isinstance(key, str) for key in dictionary.keys()):
        raise ValueError("The dictionary keys are not strings.")
    return dictionary


@task
def model_parameters_from_secrets(dictionary: Dict[str, str]) -> Dict[str, Any]:
    """
    Gets model parameters from Vault secrets.

    Args:
        dictionary (Dict[str, str]): Dictionary with the parameter name as key and the secret path
        as value. The secret must contain a single key called "value" with the parameter value.

    Returns:
        Dict[str, Any]: Dictionary with the parameter name as key and the secret value as value.
    """
    return {
        key: get_vault_secret(value)["data"]["value"]
        for key, value in dictionary.items()
    }


@task
def merge_dictionaries(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge dictionaries.
    """
    return {**dict1, **dict2}
