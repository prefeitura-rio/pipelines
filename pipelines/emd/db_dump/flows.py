"""
Database dumping flows
"""

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.emd.db_dump.tasks import sql_server_execute, sql_server_get_columns, sql_server_get_connection, sql_server_get_cursor

with Flow("dump_sql_server") as dump_sql_server_flow:

    # SQL Server parameters
    server = Parameter("sql_server_hostname")
    user = Parameter("sql_server_user")
    password = Parameter("sql_server_password")
    database = Parameter("sql_server_database")
    query = Parameter("execute_query")

    # CSV file parameters
    batch_size = Parameter("batch_size")

    # Execute query on SQL Server
    conn = sql_server_get_connection(
        server=server, user=user, password=password, database=database)
    cursor = sql_server_get_cursor(conn)
    sql_server_execute(cursor, query)

    cols = sql_server_get_columns(cursor)

    # TODO: Decidir se vai dumpar em batches ou completão
    # Se for em batch pode usar
    # >>> sql_server_fetch_batch(cursor, batch_size)

    # TODO: Criar o dataframe
    # >>> batch_to_dataframe(batch: Tuple[Tuple], columns: List[str]) -> pd.DataFrame

    # TODO: Salvar o dataframe em CSV
    # >>> dataframe_to_csv(dataframe: pd.DataFrame, path: Union[str, Path]) -> None
    # Obs: não precisa criar os diretórios pai

    # TODO: Subir os CSVs para o GCS


dump_sql_server_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sql_server_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value)
