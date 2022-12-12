# -*- coding: utf-8 -*-
"""
Flows for dumping data directly from BigQuery to GCS.
"""
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_earth_engine_asset.tasks import (
    download_data_to_gcs,
    get_project_id,
    trigger_cron_job,
    update_last_trigger,
    get_earth_engine_key_from_vault,
    create_table_asset,
)
from pipelines.utils.tasks import rename_current_flow_run_msg

with Flow(
    name=utils_constants.FLOW_DUMP_EARTH_ENGINE_ASSET_NAME.value,
    code_owners=[
        "diego",
        "gabriel",
    ],
) as dump_earth_engine_asset_flow:

    project_id = Parameter("project_id", required=False)
    query = Parameter("query", required=False)
    bd_project_mode = Parameter(
        "bd_project_mode", required=False, default="prod"
    )  # prod or staging
    billing_project_id = Parameter("billing_project_id", required=False)
    desired_crontab = Parameter(
        "desired_crontab",
        required=False,
        default="0 0 * * *",
    )

    service_account = (Parameter("service_account"),)
    vault_path_earth_engine_key = (Parameter("vault_path_earth_engine_key"),)
    gcs_asset_path = (Parameter("gcs_asset_path"),)
    ee_asset_path = (Parameter("ee_asset_path"),)

    rename_flow_run = rename_current_flow_run_msg(
        msg=f"Criar EE Asset: {ee_asset_path}",
    )

    project_id = get_project_id(project_id=project_id, bd_project_mode=bd_project_mode)

    trigger_download, execution_time = trigger_cron_job(
        project_id=project_id,
        ee_asset_path=ee_asset_path,
        cron_expression=desired_crontab,
    )

    with case(trigger_download, True):
        download_task = download_data_to_gcs(  # pylint: disable=C0103
            project_id=project_id,
            gcs_asset_path=gcs_asset_path,
            query=query,
            bd_project_mode=bd_project_mode,
            billing_project_id=billing_project_id,
        )

        update_task = update_last_trigger(  # pylint: disable=C0103
            project_id=project_id,
            gcs_asset_path=gcs_asset_path,
            execution_time=execution_time,
        )
        update_task.set_upstream(download_task)

        service_account_secret_path = get_earth_engine_key_from_vault(
            vault_path_earth_engine_key=vault_path_earth_engine_key
        )
        service_account_secret_path.set_upstream(update_task)

        create_table_asset(
            service_account=service_account,
            service_account_secret_path=service_account_secret_path,
            project_id=project_id,
            gcs_asset_path=gcs_asset_path,
            ee_asset_path=ee_asset_path,
        )

        create_table_asset.set_upstream(service_account_secret_path)


dump_earth_engine_asset_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_earth_engine_asset_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
