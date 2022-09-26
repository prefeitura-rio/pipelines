# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
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
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_CHANCE;",
    },
    "comentario": {
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_Comentario;",
    },
    "indicador": {
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_Indicador;",
    },
    "meta": {
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_Metas;",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
    },
    "nota_meta": {
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_NotaMeta;",
    },
}


egp_web_clocks = generate_dump_db_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2022, 9, 14, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    db_database="EGPWEB_PRD",
    db_host="10.2.221.101",
    db_port="1433",
    db_type="sql_server",
    dataset_id="planejamento_gestao_acordo_resultados",
    vault_secret_path="egpweb-prod",
    table_parameters=egp_web_queries,
)

egp_web_weekly_update_schedule = Schedule(clocks=untuple(egp_web_clocks))
