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
# Ergon Schedules
#
#####################################

ergon_queries = {
    "cargo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_CARGOS_",
    },
    "categoria": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_CATEGORIAS_",
    },
    "empresa": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_EMPRESAS",
    },
    "matricula": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ERG_MATRICULAS",
    },
    "fita_banco": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "MES_ANO",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FITA_BANCO",
    },
    "folha_empresa": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "MES_ANO",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FOLHAS_EMP",
    },
    "forma_provimento": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FORMAS_PROV_",
    },
    "funcionario": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FUNCIONARIOS",
    },
    "horario_trabalho": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_HORARIO_TRAB_",
    },
    "setor": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_HSETOR_",
    },
    "jornada": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_JORNADAS_",
    },
    "orgaos_externos": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ORGAOS_EXTERNOS",
    },
    "orgaos_regime_juridico": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ORGAOS_REGIMES_JUR_",
    },
    "provimento": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_PROVIMENTOS_EV",
    },
    "regime_juridico": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_REGIMES_JUR_",
    },
    "tipo_folha": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_FOLHA",
    },
    "tipo_orgao": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_ORGAO",
    },
    "tipo_vinculo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_VINC_",
    },
    "vinculo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_VINCULOS",
    },
    "licenca_afastamento": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "DTINI",
        "dump_mode": "append",
        "execute_query": """
          SELECT
NUMFUNC,NUMVINC,DTINI,DTFIM,TIPOFREQ,CODFREQ,MOTIVO,PONTPUBL,DTPREVFIM,FLEX_CAMPO_01,FLEX_CAMPO_02,FLEX_CAMPO_03,FLEX_CAMPO_04,FLEX_CAMPO_05,PONTLEI,EMP_CODIGO,FLEX_CAMPO_06,FLEX_CAMPO_07,FLEX_CAMPO_08,FLEX_CAMPO_09,FLEX_CAMPO_10,ID_REG,REQLICAFAST,RESULTPRONT,FLEX_CAMPO_11,FLEX_CAMPO_12,FLEX_CAMPO_13,FLEX_CAMPO_14,FLEX_CAMPO_15,FLEX_CAMPO_16,FLEX_CAMPO_17,FLEX_CAMPO_18,FLEX_CAMPO_19,FLEX_CAMPO_20,FLEX_CAMPO_21,FLEX_CAMPO_22,FLEX_CAMPO_23,FLEX_CAMPO_24,FLEX_CAMPO_25,FLEX_CAMPO_26,FLEX_CAMPO_27,FLEX_CAMPO_28,FLEX_CAMPO_29,FLEX_CAMPO_30,FLEX_CAMPO_31,FLEX_CAMPO_32,FLEX_CAMPO_33,FLEX_CAMPO_34,FLEX_CAMPO_35,FLEX_CAMPO_36,FLEX_CAMPO_37,FLEX_CAMPO_38,FLEX_CAMPO_39,FLEX_CAMPO_40,FLEX_CAMPO_41,FLEX_CAMPO_42,FLEX_CAMPO_43,FLEX_CAMPO_44,FLEX_CAMPO_45,PONTPROC
          FROM ERGON.LIC_AFAST
        """,
        "interval": timedelta(days=1),
    },
    #     "frequencias": {
    #       "materialize_after_dump": True,
    #       "materialization_mode": "prod",
    #       "partition_columns": "DTINI",
    #       "dump_mode": "append",
    #       "execute_query": "SELECT * FROM ERGON.frequencias",
    #       "interval": timedelta(days=1)
    #     },
}

ergon_clocks = generate_dump_db_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2022, 11, 9, 10, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    db_database="P01.PCRJ",
    db_host="10.70.6.21",
    db_port="1526",
    db_type="oracle",
    dataset_id="recursos_humanos_ergon",
    vault_secret_path="ergon-prod",
    table_parameters=ergon_queries,
)

ergon_monthly_update_schedule = Schedule(clocks=untuple(ergon_clocks))
