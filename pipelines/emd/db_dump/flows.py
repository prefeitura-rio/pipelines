"""
Database dumping flows
"""

from uuid import uuid4

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.emd.db_dump.db import Database
from pipelines.emd.db_dump.tasks import (
    create_bd_table,
    database_execute,
    database_fetch,
    database_get,
    dump_batches_to_csv,
    dump_header_to_csv,
    upload_to_gcs,
)
from pipelines.tasks import get_user_and_password
from pipelines.utils import log_task

with Flow("Ingerir tabela de banco SQL") as dump_sql_flow:

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
    batch_size = Parameter("batch_size")

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    full_dump = Parameter("full_dump", default='append') # replace or  append

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
    )
    
    wait_db_execute = database_execute(  # pylint: disable=invalid-name
        database=db_object,
        query=query,
    )

    # Create CSV file with headers
    header_path = dump_header_to_csv(
        database=db_object,
        header_path=f"data/{uuid4()}/",
        wait=wait_db_execute,
    )

    # Create table in BigQuery
    wait_create_db = create_bd_table(  # pylint: disable=invalid-name
        path=header_path,
        dataset_id=dataset_id,
        table_id=table_id,
        full_dump=full_dump,
    )

    #####################################
    #
    # Tasks section #2 - Dump batches
    #
    #####################################

    ## esta executando a query duas vezes, uma para criar o header e outra para criar os batches
    # Execute query
    # wait_db_execute = database_execute(  # pylint: disable=invalid-name
    #     database=db_object,
    #     query=query,
    #     wait=wait_create_db,
    # )

    # Dump batches to CSV files
    path = dump_batches_to_csv(
        database=db_object,
        batch_size=batch_size,
        prepath=f"data/{uuid4()}/",
        wait=wait_create_db,
    )

    # Upload to GCS
    upload_to_gcs(path=path, dataset_id=dataset_id, table_id=table_id)


dump_sql_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sql_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value)

with Flow("Executar query SQL") as run_sql_flow:

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
    wait_db_execute = database_execute(  # pylint: disable=invalid-name
        database=db_object,
        query=query,
    )

    # Get results
    results = database_fetch(
        database=db_object,
        batch_size=batch_size,
        wait=wait_db_execute,
    )

    # Log results
    log_task(results)


run_sql_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_sql_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value)
