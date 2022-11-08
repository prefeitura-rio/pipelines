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
from pipelines.utils.dump_db.db import Database
from pipelines.utils.tasks import (
    get_current_flow_labels,
    get_user_and_password,
    greater_than,
    rename_current_flow_run_dataset_table,
    create_table_and_upload_to_gcs,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_db.tasks import (
    database_execute,
    database_fetch,
    database_get,
    dump_batches_to_file,
    format_partitioned_query,
    parse_comma_separated_string_to_list,
)
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants

with Flow(
    name=utils_constants.FLOW_DUMP_DB_NAME.value,
    code_owners=[
        "diego",
        "gabriel",
    ],
) as dump_sql_flow:

    #####################################
    #
    # Parameters
    #
    #####################################

    # DBMS parameters
    hostname = Parameter("db_host")
    port = Parameter("db_port")
    database = Parameter("db_database")
    database_type = Parameter("db_type")
    query = Parameter("execute_query")
    partition_columns = Parameter("partition_columns", required=False, default="")
    partition_date_format = Parameter(
        "partition_date_format", required=False, default="%Y-%m-%d"
    )
    lower_bound_date = Parameter("lower_bound_date", required=False)

    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    # Use Vault for credentials
    secret_path = Parameter("vault_secret_path")

    # Data file parameters
    batch_size = Parameter("batch_size", default=50000)

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    dump_mode = Parameter("dump_mode", default="append")  # overwrite or append
    batch_data_type = Parameter("batch_data_type", default="csv")  # csv or parquet
    dbt_model_secret_parameters = Parameter(
        "dbt_model_secret_parameters", default={"hash_seed": "hash_seed"}
    )
    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    #####################################
    #
    # Tasks section #0 - Get credentials
    #
    #####################################

    # Get credentials from Vault
    user, password = get_user_and_password(
        secret_path=secret_path, wait=rename_flow_run
    )

    #####################################
    #
    # Tasks section #1 - Create table
    #
    #####################################

    # Get current flow labels
    current_flow_labels = get_current_flow_labels()

    # Parse partition columns
    partition_columns = parse_comma_separated_string_to_list(text=partition_columns)

    # Execute query on SQL Server
    db_object: Database = database_get(
        database_type=database_type,
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
        wait=user,
    )

    # Format partitioned query if required
    formated_query = format_partitioned_query(
        query=query,
        dataset_id=dataset_id,
        table_id=table_id,
        partition_columns=partition_columns,
        lower_bound_date=lower_bound_date,
        date_format=partition_date_format,
        wait=db_object,
    )

    db_execute = database_execute(  # pylint: disable=invalid-name
        database=db_object,
        query=formated_query,
        wait=formated_query,
        flow_name="dump_db",
        labels=current_flow_labels,
        dataset_id=dataset_id,
        table_id=table_id,
    )

    # Dump batches to files
    batches_path, num_batches = dump_batches_to_file(
        database=db_object,
        batch_size=batch_size,
        prepath=f"data/{uuid4()}/",
        partition_columns=partition_columns,
        batch_data_type=batch_data_type,
        wait=db_execute,
        flow_name="dump_db",
        labels=current_flow_labels,
        dataset_id=dataset_id,
        table_id=table_id,
    )

    data_exists = greater_than(num_batches, 0)

    with case(data_exists, True):

        create_table_and_upload_to_gcs(
            data_path=batches_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            wait=data_exists,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "materialize_to_datario": materialize_to_datario,
                    "dbt_model_secret_parameters": dbt_model_secret_parameters,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
            )

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

            with case(dump_to_gcs, True):
                # Trigger Dump to GCS flow run with project id as datario
                dump_to_gcs_flow = create_flow_run(
                    flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                    project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                    parameters={
                        "project_id": "datario",
                        "dataset_id": dataset_id,
                        "table_id": table_id,
                        "maximum_bytes_processed": maximum_bytes_processed,
                    },
                    labels=[
                        "datario",
                    ],
                    run_name=f"Dump to GCS {dataset_id}.{table_id}",
                )
                dump_to_gcs_flow.set_upstream(wait_for_materialization)

                wait_for_dump_to_gcs = wait_for_flow_run(
                    dump_to_gcs_flow,
                    stream_states=True,
                    stream_logs=True,
                    raise_final_state=True,
                )


dump_sql_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sql_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)


with Flow(
    name="EMD: template - Executar query SQL",
    code_owners=[
        "diego",
        "gabriel",
    ],
) as run_sql_flow:
    #####################################
    #
    # Parameters
    #
    #####################################

    # SQL Server parameters
    hostname = Parameter("db_host")
    port = Parameter("db_port")
    database = Parameter("db_database")
    database_type = Parameter("db_type")
    query = Parameter("execute_query")

    # Use Vault for credentials
    secret_path = Parameter("vault_secret_path")

    # Data file parameters
    batch_size = Parameter("no_of_rows", default="all")

    #####################################
    #
    # Tasks section #0 - Get credentials
    #
    #####################################

    # Get credentials from Vault
    user, password = get_user_and_password(secret_path=secret_path)

    #####################################
    #
    # Tasks section #1 - Execute query
    #
    #####################################

    # Execute query on SQL Server
    db_object: Database = database_get(
        database_type=database_type,
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    db_execute = database_execute(  # pylint: disable=invalid-name
        database=db_object,
        query=query,
        flow_name="execute_sql",
    )

    # Log results
    database_fetch(
        database=db_object,
        batch_size=batch_size,
        wait=db_execute,
        flow_name="execute_sql",
    )
run_sql_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_sql_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
