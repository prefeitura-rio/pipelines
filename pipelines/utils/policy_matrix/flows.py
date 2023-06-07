# -*- coding: utf-8 -*-
# pylint: disable=invalid-name, E1101
"""
Flow for generating policy matrix
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.utils.decorators import Flow
from pipelines.utils.dump_db.tasks import (
    parse_comma_separated_string_to_list,
)
from pipelines.utils.policy_matrix.tasks import (
    get_discovery_api,
    get_iam_policy,
    merge_iam_policies,
    generate_roles_matrix,
    roles_matrix_to_pandas_dataframe,
)

from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_now_time,
    rename_current_flow_run_now_time,
)

with Flow(
    "EMD: utils - Gerar matriz de políticas de acesso",
    code_owners=[
        "gabriel",
        "diego",
    ],
) as utils_policy_matrix_flow:

    # Parameters
    project_ids = Parameter("project_ids", default="rj-escritorio,rj-escritorio-dev")
    mode = Parameter("mode", default="prod")

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="Matrix Acessos: ", now_time=get_now_time()
    )

    discovery_api = get_discovery_api(mode=mode)
    discovery_api.set_upstream(rename_flow_run)

    project_ids_list = parse_comma_separated_string_to_list(text=project_ids)
    project_ids_list.set_upstream(discovery_api)

    policies = get_iam_policy(project_ids=project_ids_list, discovery_api=discovery_api)
    policies.set_upstream(project_ids_list)

    merged_policies = merge_iam_policies(
        project_ids=project_ids_list, policies=policies
    )
    merged_policies.set_upstream(policies)

    role_matrix = generate_roles_matrix(policies=merged_policies)
    role_matrix.set_upstream(merged_policies)

    save_file_path = roles_matrix_to_pandas_dataframe(roles_matrix=role_matrix)
    save_file_path.set_upstream(role_matrix)

    create_and_upload_to_gcs = create_table_and_upload_to_gcs(
        data_path=save_file_path,
        dataset_id="datalake_policy",
        table_id="permissoes",
        dump_mode="overwrite",
        biglake_table=False,
    )
    create_and_upload_to_gcs.set_upstream(save_file_path)

utils_policy_matrix_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
utils_policy_matrix_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
