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

QUERY_SICOP_APENSO = """
SELECT CHAVE, NUM_PROCESSO_PRINCIPAL, NUM_PROCESSO_APENSADO, I22005_COD_OPER FROM SICOP.VW_APENSO;
"""
QUERY_SICOP_ASSUNTO = """
SELECT IDENT, COD, SEQ, DESC_ASSUNTO FROM SICOP.VW_ASSUNTO;
"""
QUERY_SICOP_DOCUMENTO = """
SELECT ORG_RESP, ORG_TRANSC, TIPO_DOC, SEC_SIST2, ANO_SIST2, MES_SIST2, DIA_SIST2,
NUM_DOCUMENTO, REQUERENTE, COD_ASSUN, MAT_TRANSC, ASSUN_COMP FROM SICOP.VW_DOCUMENTO;
"""
QUERY_SICOP_ORGAO = """
SELECT ORG_SICOP, COD_ORCTO FROM SICOP.VW_ORGAO;
"""
QUERY_SICOP_PROCESSO = """
SELECT ORG_TRANSC, NUM_PROCESSO, CPF_CGC, IDENT, REQUERENTE, DIA_PROC2, MES_PROC2, SEC_PROC2,
ANO_PROC2, COD_ASSU_P, ANO_SIST2, SEC_SIST2, MES_SIST2, DIA_SIST2, DESC_ASSUN, MAT_TRANSC, STATUS
FROM SICOP.VW_PROCESSO;
"""
QUERY_SICOP_TRAMITACAO = """
SELECT ORG_RESP, TIPO_DOC, NUM_DOCUMENTO, SEQ, SEC_REMES2, ANO_REMES2, MES_REMES2,
DIA_REMES2, DESTINO, COD_DESP FROM SICOP.VW_TRAMITACAO_DOCUMENTO;
"""

_sicop_queries = {
    "sicop_apenso": {
        #        "partition_columns": "dt_inicio",
        #        "lower_bound_date": "2021-01-01",
        "materialize_after_dump": False,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": QUERY_SICOP_APENSO,
    },
    "sicop_assunto": {
        "materialize_after_dump": False,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": QUERY_SICOP_ASSUNTO,
    },
    "sicop_documento": {
        "materialize_after_dump": False,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": QUERY_SICOP_DOCUMENTO,
    },
    "sicop_orgao": {
        "materialize_after_dump": False,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": QUERY_SICOP_ORGAO,
    },
    "sicop_processo": {
        "materialize_after_dump": False,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": QUERY_SICOP_PROCESSO,
    },
    "sicop_tramitacao": {
        "materialize_after_dump": False,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": QUERY_SICOP_TRAMITACAO,
    },
}
_sicop_infra_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 5, 14, 2, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
    db_database="CP01.smf",
    db_host="10.90.31.22",
    db_port="1521",
    db_type="oracle",
    dataset_id="adm_processo_interno_sicop",  # trocar o dataset_id
    vault_secret_path="sicop-sql",
    table_parameters=_sicop_queries,
    #
)

_processorio_infra_daily_update_schedule = Schedule(clocks=untuple(_sicop_infra_clocks))
