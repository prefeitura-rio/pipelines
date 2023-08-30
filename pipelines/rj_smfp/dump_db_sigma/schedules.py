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

_sigma_queries = {
    "sancao_fornecedor": {
        "biglake_table": True,
        # "materialize_after_dump": True,
        # "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        # "partition_columns": "anoInscricao",
        # "partition_date_format": "%Y",
        # "lower_bound_date": "current_year",
        "execute_query": """
            SELECT
                CPF_CNPJ,
                RAZAO_SOCIAL,
                NR_ORDEM,
                PROCESSO_ORIGEM,
                PROCESSO_INSTRUTIVO,
                PROCESSO_FATURA,
                CD_SANCAO,
                DS_SANCAO,
                DT_SANCAO,
                DT_EXTINCAO_SANCAO
            FROM SIGMA.VW_SANCAO_ADMINISTRATIVA
        """,  # noqa
    },
}

sigma_infra_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 3, 21, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    db_database="CP01.SMF",
    db_host="10.90.31.22",
    db_port="1521",
    db_type="oracle",
    dataset_id="adm_orcamento_sigma",
    vault_secret_path="db-sigma",
    table_parameters=_sigma_queries,
)

sigma_daily_update_schedule = Schedule(clocks=untuple(sigma_infra_clocks))
