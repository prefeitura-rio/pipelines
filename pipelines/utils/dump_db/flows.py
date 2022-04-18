"""
Database dumping flows
"""

from functools import partial
from uuid import uuid4

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.dump_db.db import Database

from pipelines.utils.tasks import (
    create_bd_table,
    upload_to_gcs,
    dump_header_to_csv,
)
from pipelines.utils.dump_db.tasks import (
    database_execute,
    database_fetch,
    database_get,
    dump_batches_to_csv,
    format_partitioned_query,
)
from pipelines.utils.tasks import get_user_and_password
from pipelines.utils.utils import notify_discord_on_failure

with Flow(
    name="EMD: template - Ingerir tabela de banco SQL",
    on_failure=partial(
        notify_discord_on_failure,
        secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value,
    ),
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
    partition_column = Parameter("partition_column", required=False)
    lower_bound_date = Parameter("lower_bound_date", required=False)

    # Use Vault for credentials
    secret_path = Parameter("vault_secret_path")

    # CSV file parameters
    batch_size = Parameter("batch_size", default=50000)

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    dump_type = Parameter("dump_type", default="append")  # overwrite or append

    #####################################
    #
    # Tasks section #0 - Get credentials
    #
    #####################################

    # Get credentials from Vault
    user, password = get_user_and_password(secret_path=secret_path)

    #####################################
    #
    # Tasks section #1 - Create table
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
        wait=user,
    )

    # Format partitioned query if required
    formated_query = format_partitioned_query(
        query=query,
        dataset_id=dataset_id,
        table_id=table_id,
        partition_column=partition_column,
        lower_bound_date=lower_bound_date,
        wait=db_object,
    )

    db_execute = database_execute(  # pylint: disable=invalid-name
        database=db_object,
        query=formated_query,
        wait=formated_query,
    )

    # Dump batches to CSV files
    batches_path = dump_batches_to_csv(
        database=db_object,
        batch_size=batch_size,
        prepath=f"data/{uuid4()}/",
        partition_column=partition_column,
        wait=db_execute,
    )

    # TODO: Skip all downstream tasks if no data was dumped

    # Create CSV file with headers
    header_path = dump_header_to_csv(
        data_path=batches_path,
        wait=batches_path,
    )

    # Create table in BigQuery
    create_db = create_bd_table(  # pylint: disable=invalid-name
        path=header_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type=dump_type,
        wait=header_path,
    )

    #####################################
    #
    # Tasks section #2 - Dump batches
    #
    #####################################

    # Upload to GCS
    upload_to_gcs(
        path=batches_path,
        dataset_id=dataset_id,
        table_id=table_id,
        wait=create_db,
    )


dump_sql_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sql_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)


with Flow(
    name="EMD: template - Executar query SQL",
    on_failure=partial(
        notify_discord_on_failure,
        secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value,
    ),
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

    # CSV file parameters
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
    )

    # Log results
    database_fetch(
        database=db_object,
        batch_size=batch_size,
        wait=db_execute,
    )
run_sql_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_sql_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
