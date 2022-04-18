"""
Database dumping flows
"""

from uuid import uuid4

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.emd.db_dump.tasks import (
    dump_batches_to_csv,
    dump_header_to_csv,
    sql_server_execute,
    sql_server_get_connection,
    sql_server_get_cursor,
    upload_to_gcs,
    create_bd_table,
)
from pipelines.tasks import get_user_and_password

with Flow("dump_sql_server") as dump_sql_server_flow:

    # SQL Server parameters
    server = Parameter("sql_server_hostname")
    database = Parameter("sql_server_database")
    query = Parameter("execute_query")

    # Use Vault for credentials
    secret_path = Parameter("vault_secret_path")

    # CSV file parameters
    batch_size = Parameter("batch_size")

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

    # Get credentials from Vault
    user, password = get_user_and_password(secret_path=secret_path)

    # Execute query on SQL Server
    conn = sql_server_get_connection(
        server=server, user=user, password=password, database=database
    )
    cursor = sql_server_get_cursor(connection=conn)
    cursor = sql_server_execute(cursor=cursor, query=query)

    # Dump batches to CSV files
    path = dump_batches_to_csv(
        cursor=cursor, batch_size=batch_size, prepath=f"data/{uuid4()}/"
    )

    # Upload to GCS
    upload_to_gcs(path=path, dataset_id=dataset_id, table_id=table_id)


dump_sql_server_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sql_server_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value)

with Flow("create_table_from_header") as create_table_from_header_flow:

    # SQL Server parameters
    server = Parameter("sql_server_hostname")
    user = Parameter("sql_server_user")
    query = Parameter("execute_query")

    # Use Vault for credentials
    secret_path = Parameter("vault_secret_path")

    # CSV file parameters
    batch_size = Parameter("batch_size")

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

    # Get credentials from Vault
    user, password = get_user_and_password(secret_path=secret_path)

    # Execute query on SQL Server
    conn = sql_server_get_connection(
        server=server, user=user, password=password, database=database
    )
    cursor = sql_server_get_cursor(connection=conn)
    cursor = sql_server_execute(cursor=cursor, query=query)

    # Create CSV file with headers
    header_path = dump_header_to_csv(
        cursor=cursor, header_path=f"data/{uuid4()}/")
    create_bd_table(path=header_path, dataset_id=dataset_id, table_id=table_id)

create_table_from_header_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
create_table_from_header_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
