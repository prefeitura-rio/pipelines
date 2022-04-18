"""
Database dumping flows
"""

from uuid import uuid4

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.emd.db_dump.schedules import (
    ergon_monthly_update_schedule,
    sme_daily_update_schedule,
    _1746_daily_update_schedule,
)
from pipelines.emd.db_dump.flows_functions import _dump_sql_flow, _run_sql_flow


with Flow("EMD: Ingerir tabela de banco SQL") as dump_sql_flow:
    _dump_sql_flow()


dump_sql_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sql_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

with Flow("EMD: Ingerir tabelas Ergon") as dump_ergon_flow:
    _dump_sql_flow()

dump_ergon_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_ergon_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
dump_ergon_flow.schedule = ergon_monthly_update_schedule

with Flow("SME: Ingerir tabelas SME") as dump_sme_flow:
    _dump_sql_flow()

dump_sme_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sme_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
dump_sme_flow.schedule = sme_daily_update_schedule

with Flow("1746: Ingerir tabela 1746") as dump_1746_flow:
    _dump_sql_flow()

dump_1746_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_1746_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
dump_1746_flow.schedule = _1746_daily_update_schedule

with Flow("EMD: Executar query SQL") as run_sql_flow:
    _run_sql_flow()

run_sql_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_sql_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
