# -*- coding: utf-8 -*-
"""
Tasks for the policy matrix generation.
"""
from typing import Dict, List, Union

from basedosdados.upload.base import Base
import googleapiclient.discovery
import pandas as pd
from prefect import task


@task
def get_discovery_api(mode: str = "prod") -> googleapiclient.discovery.Resource:
    """
    Get the discovery API for the given mode.
    """
    base = Base()
    credentials = base._load_credentials(mode=mode)  # pylint: disable=W0212
    return googleapiclient.discovery.build(
        "cloudresourcemanager", "v1", credentials=credentials
    )


@task
def get_iam_policy(
    project_id: str, discovery_api: googleapiclient.discovery.Resource
) -> Dict[str, Union[int, str, List[Dict[str, Union[str, List[str]]]]]]:
    """
    Get the IAM policy for the given project.

    Output format is:
    {
        "version": 1 (int),
        "etag": etag (str),
        "bindings": [
            {
                "role": role (str),
                "members": [member (str), ...]
            },
            ...
        ]
    }
    """
    return (
        discovery_api.projects()
        .getIamPolicy(
            resource=project_id, body={"options": {"requestedPolicyVersion": 1}}
        )
        .execute()
    )


@task
def merge_iam_policies(
    project_ids: List[str],
    policies: List[Dict[str, Union[int, str, List[Dict[str, Union[str, List[str]]]]]]],
) -> Dict[str, List[Dict[str, Union[str, List[str]]]]]:
    """
    Merges IAM policies from different projects into one dictionary in the format:
    {
        "project_id": [
            {
                "role": role (str),
                "members": [member (str), ...]
            },
            ...
        ],
        ...
    }
    """
    return {
        project_id: policy["bindings"]
        for project_id, policy in zip(project_ids, policies)
    }


@task
def generate_roles_matrix(
    policies: Dict[str, List[Dict[str, Union[str, List[str]]]]]
) -> Dict[str, Dict[str, List[str]]]:
    """
    Generates a roles matrix from the given IAM policies in the format:
    {
        "project_id": {
            "member": [role, ...],
            ...
        },
        ...
    }
    """
    roles_matrix = {}
    for project_id, bindings in policies.items():
        roles_matrix[project_id] = {}
        for binding in bindings:
            for member in binding["members"]:
                if member not in roles_matrix[project_id]:
                    roles_matrix[project_id][member] = []
                roles_matrix[project_id][member].append(binding["role"])
    return roles_matrix


@task
def roles_matrix_to_pandas_dataframe(
    roles_matrix: Dict[str, Dict[str, List[str]]]
) -> pd.DataFrame:
    """
    Converts the roles matrix to a pandas dataframe with the following format:
    |             | project_id_1 | project_id_2 | ... |
    |-------------|--------------|--------------|-----|
    | member_1    | role_1       | role_2       | ... |
    | member_2    | role_1       | role_2       | ... |
    | ...         | ...          | ...          | ... |
    """
    return pd.DataFrame.from_dict(roles_matrix).T
