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

_processorio_infra_query = {
    "classificacao": {
        # "partition_columns": "dt_inicio",
        # "lower_bound_date": "2021-01-01",
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": "SELECT ID_CLASSIFICACAO, DESCR_CLASSIFICACAO, HIS_ATIVO, CODIFICACAO FROM SIGA.VW_CLASSIFICACAO",  # noqa
    },
    "documento_tempo": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": "SELECT SIGLA_DOC, DT_PRIMEIRAASSINATURA, ULTIMO_ID_MOV, DT_FINALIZACAO, ARQUIVADO, ID_MOBIL, TEMPO_TRAMITACAO, ID_LOTA_RESP, LOTACAO_RESP, DATA_COM_RESP_ATUAL FROM SIGA.DOCUMENTOS_TEMPO",  # noqa
    },
    "forma_documento": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": "SELECT ID_FORMA_DOC, DESCR_FORMA_DOC, SIGLA_FORMA_DOC, ID_TIPO_FORMA_DOC FROM SIGA.VW_FORMA_DOCUMENTO",  # noqa
    },
    "lotacao": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": "SELECT ID_LOTACAO, DATA_INI_LOT, DATA_FIM_LOT, NOME_LOTACAO, ID_LOTACAO_PAI, SIGLA_LOTACAO, ID_ORGAO_USU, IS_EXTERNA_LOTACAO FROM CORPORATIVO.VW_LOTACAO",  # noqa
    },
    "mobil": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": "SELECT ID_MOBIL, ID_DOC FROM SIGA.VW_MOBIL",  # noqa
    },
    "modelo": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": "SELECT ID_MOD, NM_MOD, DESC_MOD, HIS_ID_INI, HIS_IDC_INI, HIS_IDC_FIM, HIS_ATIVO, IS_PETICIONAMENTO FROM SIGA.VW_MODELO",  # noqa
    },
    "movimentacao": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": "SELECT ID_MOV, ID_TP_MOV, ID_CADASTRANTE, ID_LOTA_CADASTRANTE, DT_MOV, DT_FIM_MOV, ID_MOV_REF, ID_MOBIL FROM SIGA.VW_MOVIMENTACAO",  # noqa
    },
    "nivel_acesso": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": "SELECT ID_NIVEL_ACESSO, NM_NIVEL_ACESSO FROM SIGA.VW_NIVEL_ACESSO",  # noqa
    },
    "orgao_usuario": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": "SELECT ID_ORGAO_USU, NM_ORGAO_USU, SIGLA_ORGAO_USU, COD_ORGAO_USU, IS_EXTERNO_ORGAO_USU, HIS_ATIVO, IS_PETICIONAMENTO FROM CORPORATIVO.VW_ORGAO_USUARIO",  # noqa
    },
}

processorio_infra_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 3, 21, 2, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
    db_database="SIGADOC.PCRJ",
    db_host="10.70.6.63",
    db_port="1521",
    db_type="oracle",
    dataset_id="adm_processo_interno_processorio",
    vault_secret_path="processorio-sql",
    table_parameters=_processorio_infra_query,
)

processorio_infra_daily_update_schedule = Schedule(
    clocks=untuple(processorio_infra_clocks)
)
