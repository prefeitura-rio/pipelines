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
    create_table_asset,
)
from pipelines.utils.tasks import rename_current_flow_run_dataset_table

with Flow(
    name=utils_constants.FLOW_DUMP_EARTH_ENGINE_ASSET_NAME.value,
    code_owners=[
        "diego",
        "gabriel",
    ],
) as dump_earth_engine_asset_flow:

    project_id = Parameter("project_id", required=False)
    dataset_id = Parameter("dataset_id")  # dataset_id or dataset_id_staging
    table_id = Parameter("table_id")
    query = Parameter("query", required=False)
    jinja_query_params = Parameter("jinja_query_params", required=False)
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
    service_account_secret_path = (Parameter("service_account_secret_path"),)
    gcs_asset_path = (Parameter("gcs_asset_path"),)
    ee_asset_path = (Parameter("ee_asset_path"),)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump to GCS: ", dataset_id=dataset_id, table_id=table_id
    )

    project_id = get_project_id(project_id=project_id, bd_project_mode=bd_project_mode)

    trigger_download, execution_time = trigger_cron_job(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        cron_expression=desired_crontab,
    )

    with case(trigger_download, True):
        download_task = download_data_to_gcs(  # pylint: disable=C0103
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            query=query,
            jinja_query_params=jinja_query_params,
            bd_project_mode=bd_project_mode,
            billing_project_id=billing_project_id,
        )

        update_task = update_last_trigger(  # pylint: disable=C0103
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            execution_time=execution_time,
        )
        update_task.set_upstream(download_task)

        create_table_asset(
            service_account=service_account,
            service_account_secret_path=service_account_secret_path,
            project_id=project_id,
            gcs_asset_path=gcs_asset_path,
            ee_asset_path=ee_asset_path,
        )

        create_table_asset.set_upstream(update_task)


dump_earth_engine_asset_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_earth_engine_asset_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
