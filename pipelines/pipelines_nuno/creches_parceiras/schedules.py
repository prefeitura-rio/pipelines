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
# SME Schedules
#
#####################################

sme_queries = {
    "creches_parceiras": {
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "dbt_model_secret_parameters": {"hash_seed": "hash_seed"},
        "execute_query": """
            SELECT [esc_id]
                  ,[esc_codigo]
                  ,[esc_nome]
                  ,[esc_codigoInep]
                  ,[tre_id]
                  ,[esc_situacao]
            FROM [GestaoEscolar].[dbo].[ESC_Escola]
            WHERE tre_id = 5 AND esc_situacao = 1
        """,
    },
}

sme_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 11, 4, 18, 00, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SME_AGENT_LABEL.value,
    ],
    db_database="GestaoEscolar",
    db_host="10.70.6.103",
    db_port="1433",
    db_type="sql_server",
    dataset_id="educacao_basica_staging",
    vault_secret_path="clustersqlsme",
    table_parameters=sme_queries,
)

sme_educacao_basica_daily_update_schedule = Schedule(clocks=untuple(sme_clocks))
