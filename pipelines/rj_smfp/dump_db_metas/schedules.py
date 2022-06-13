# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
import pytz

from pipelines.constants import constants
from pipelines.utils.dump_db.utils import generate_dump_db_schedules
from pipelines.utils.utils import untuple_clocks as untuple

#####################################
#
# EGPWeb Schedules
#
#####################################

egp_web_queries = {
    "chance": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_CHANCE;",
    },
    "comentario": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_Comentario;",
    },
    "indicador": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_Indicador;",
    },
    "metas": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_Metas;",
    },
    "notameta": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_NotaMeta;",
    },
}


egp_web_clocks = generate_dump_db_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2022, 6, 13, 10, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    db_database="EGPWEB_PRD",
    db_host="10.2.221.101",
    db_port="1433",
    db_type="sql_server",
    dataset_id="administracao_metas",
    vault_secret_path="egpweb-prod",
    table_parameters=egp_web_queries,
)

egp_web_monthly_update_schedule = Schedule(clocks=untuple(egp_web_clocks))
