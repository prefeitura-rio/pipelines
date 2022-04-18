"""
Database dumping flows for sme project
"""

from copy import deepcopy

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_sme.dump_db.schedules import sme_educacao_basica_daily_update_schedule
from pipelines.utils.dump_db.flows import dump_sql_flow


dump_sme_flow = deepcopy(dump_sql_flow)
with dump_sql_flow as dump_sql_flow:
    hostname = Parameter("db_host", default='"10.70.6.103"')
    port = Parameter("db_port", default="1433")
    database = Parameter("db_database", default="GestaoEscolar")
    secret_path = Parameter("vault_secret_path", default="clustersqlsme")

dump_sme_flow.name = "SME: educacao_basica - Ingerir tabelas de banco SQL"
dump_sme_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sme_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
dump_sme_flow.schedule = sme_educacao_basica_daily_update_schedule
