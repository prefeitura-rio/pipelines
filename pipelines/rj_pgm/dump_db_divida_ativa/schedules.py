# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz

from pipelines.constants import constants
from pipelines.utils.dump_db.utils import generate_dump_db_schedules
from pipelines.utils.utils import untuple_clocks as untuple


#####################################
#
# Processorio Schedules
#
#####################################

_dam_queries = {
    "inscritos_divida_ativa": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "append",
        "partition_columns": "anoInscricao",
        "partition_date_format": "%Y",
        "lower_bound_date": "current_year",
        "execute_query": """
            SELECT
                Nome,
                Cpf_Cnpj,
                anoInscricao,
                valDebito
            FROM DAM_PRD.dbo.vwInscritosDividaAtiva;
        """,  # noqa
    },
}

dam_infra_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 3, 21, 3, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_PGM_AGENT_LABEL.value,
    ],
    db_database="DAM_PRD",
    db_host="10.2.221.127",
    db_port="1433",
    db_type="sqlserver",
    dataset_id="adm_financas_dam",
    vault_secret_path="dam-prod",
    table_parameters=_dam_queries,
)

divida_ativa_daily_update_schedule = Schedule(clocks=untuple(dam_infra_clocks))
