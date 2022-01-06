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
    sql_server_execute,
    sql_server_get_connection,
    sql_server_get_cursor,
    upload_to_gcs,
)

with Flow("dump_sql_server") as dump_sql_server_flow:

    # SQL Server parameters
    server = Parameter("sql_server_hostname")
    user = Parameter("sql_server_user")
    password = Parameter("sql_server_password")
    database = Parameter("sql_server_database")
    query = Parameter("execute_query")

    # CSV file parameters
    batch_size = Parameter("batch_size")

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

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
dump_sql_server_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
