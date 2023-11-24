# -*- coding: utf-8 -*-
"""
Database dumping flows
"""

from datetime import timedelta
from uuid import uuid4

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_mongo.tasks import (
    database_get,
    dump_batches_to_file,
)
from pipelines.utils.tasks import (
    get_connection_string,
    get_current_flow_labels,
    greater_than,
    rename_current_flow_run_dataset_table,
    create_table_and_upload_to_gcs,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants

with Flow(
    name=utils_constants.FLOW_DUMP_MONGODB_NAME.value,
    code_owners=[
        "gabriel",
    ],
) as utils__dump_mongo_flow:
    #####################################
    #
    # Parameters
    #
    #####################################

    # DBMS
    db_host = Parameter("db_host")
    db_port = Parameter("db_port")
    db_database = Parameter("db_database")
    db_collection = Parameter("db_collection")
    db_connection_string_secret_path = Parameter("db_connection_string_secret_path")

    # Filtering and partitioning
    date_field = Parameter("date_field", required=False)
    date_lower_bound = Parameter("date_lower_bound", required=False)

    # Dumping to files
    dump_batch_size = Parameter("dump_batch_size", default=50000, required=False)

    # Uploading to BigQuery
    bq_dataset_id = Parameter("bq_dataset_id")
    bq_table_id = Parameter("bq_table_id")
    bq_upload_mode = Parameter("bq_upload_mode", default="append", required=False)
    bq_biglake_table = Parameter("bq_biglake_table", default=False, required=False)
    bq_batch_data_type = Parameter(
        "bq_batch_data_type", default="csv", required=False
    )  # csv or parquet

    # Materialization
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialize_mode = Parameter(
        "materialize_mode", default="dev", required=False
    )  # dev or prod
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    materialize_dbt_model_secret_parameters = Parameter(
        "materialize_dbt_model_secret_parameters", default={}, required=False
    )
    materialize_dbt_alias = Parameter(
        "materialize_dbt_alias", default=False, required=False
    )

    # Dumping to GCS
    gcs_dump = Parameter("gcs_dump", default=False, required=False)
    gcs_maximum_bytes_processed = Parameter(
        "gcs_maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=bq_dataset_id, table_id=bq_table_id
    )

    #####################################
    #
    # Tasks section #0 - Get credentials
    #
    #####################################

    # Get credentials from Vault
    connection_string = get_connection_string(
        secret_path=db_connection_string_secret_path
    )

    #####################################
    #
    # Tasks section #1 - Create table
    #
    #####################################

    # Get current flow labels
    current_flow_labels = get_current_flow_labels()

    # Execute query on SQL Server
    db_object = database_get(
        connection_string=connection_string,
        database=db_database,
        collection=db_collection,
    )

    # Dump batches to files
    batches_path, num_batches = dump_batches_to_file(
        database=db_object,
        batch_size=dump_batch_size,
        prepath=f"data/{uuid4()}/",
        date_field=date_field,
        date_lower_bound=date_lower_bound,
        batch_data_type=bq_batch_data_type,
    )

    data_exists = greater_than(num_batches, 0)

    with case(data_exists, True):
        upload_table = create_table_and_upload_to_gcs(
            data_path=batches_path,
            dataset_id=bq_dataset_id,
            table_id=bq_table_id,
            dump_mode=bq_upload_mode,
            biglake_table=bq_biglake_table,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": bq_dataset_id,
                    "table_id": bq_table_id,
                    "mode": materialize_mode,
                    "materialize_to_datario": materialize_to_datario,
                    "dbt_model_secret_parameters": materialize_dbt_model_secret_parameters,
                    "dbt_alias": materialize_dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {bq_dataset_id}.{bq_table_id}",
            )
            materialization_flow.set_upstream(upload_table)
            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )

            with case(gcs_dump, True):
                # Trigger Dump to GCS flow run with project id as datario
                dump_to_gcs_flow = create_flow_run(
                    flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                    project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                    parameters={
                        "project_id": "datario",
                        "dataset_id": bq_dataset_id,
                        "table_id": bq_table_id,
                        "maximum_bytes_processed": gcs_maximum_bytes_processed,
                    },
                    labels=[
                        "datario",
                    ],
                    run_name=f"Dump to GCS {bq_dataset_id}.{bq_table_id}",
                )
                dump_to_gcs_flow.set_upstream(wait_for_materialization)

                wait_for_dump_to_gcs = wait_for_flow_run(
                    dump_to_gcs_flow,
                    stream_states=True,
                    stream_logs=True,
                    raise_final_state=True,
                )


utils__dump_mongo_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
utils__dump_mongo_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
