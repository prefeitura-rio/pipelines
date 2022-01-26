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
    dump_batches_to_csv,
    dump_header_to_csv,
    database_get,
    database_execute,
    upload_to_gcs,
    create_bd_table,
)
from pipelines.tasks import get_user_and_password

with Flow("Ingerir tabela de banco SQL") as dump_sql_flow:

    #####################################
    #
    # Parameters
    #
    #####################################

    # SQL Server parameters
    hostname = Parameter("db_host")
    port = Parameter("db_port", default=None)
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
    database_execute(
        database=db_object,
        query=query,
    )

    # Create CSV file with headers
    header_path = dump_header_to_csv(
        database=db_object,
        header_path=f"data/{uuid4()}/",
    )

    # Create table in BigQuery
    create_bd_table(
        path=header_path,
        dataset_id=dataset_id,
        table_id=table_id,
    )

    #####################################
    #
    # Tasks section #2 - Dump batches
    #
    #####################################

    # Execute query
    database_execute(
        database=db_object,
        query=query,
    )

    # Dump batches to CSV files
    path = dump_batches_to_csv(
        database=db_object,
        batch_size=batch_size,
        prepath=f"data/{uuid4()}/",
    )

    # Upload to GCS
    upload_to_gcs(path=path, dataset_id=dataset_id, table_id=table_id)


dump_sql_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sql_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value)
