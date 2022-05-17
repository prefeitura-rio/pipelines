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
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_CARGOS_",
    },
    "categoria": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_CATEGORIAS_",
    },
    # "empresa": {
    #     "dump_type": "overwrite",
    #     "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_EMPRESAS",
    # },
    "matricula": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ERG_MATRICULAS",
    },
    "fita_banco": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "MES_ANO",
        "dump_type": "append",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FITA_BANCO",
    },
    # "folha_empresa": {
    #     "partition_columns": "MES_ANO",
    #     "dump_type": "append",
    #     "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FOLHAS_EMP",
    # },
    "forma_provimento": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FORMAS_PROV_",
    },
    "funcionario": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FUNCIONARIOS",
    },
    "horario_trabalho": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_HORARIO_TRAB_",
    },
    "setor": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_HSETOR_",
    },
    "jornada": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_JORNADAS_",
    },
    "orgaos_externos": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ORGAOS_EXTERNOS",
    },
    "orgaos_regime_juridico": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ORGAOS_REGIMES_JUR_",
    },
    "provimento": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_PROVIMENTOS_EV",
    },
    "regime_juridico": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_REGIMES_JUR_",
    },
    # "tipo_folha": {
    #     "dump_type": "overwrite",
    #     "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_FOLHA",
    # },
    "tipo_orgao": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_ORGAO",
    },
    "tipo_vinculo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_VINC_",
    },
    "vinculo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_VINCULOS",
    },
}


ergon_clocks = generate_dump_db_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2022, 5, 13, 11, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    db_database="P01.PCRJ",
    db_host="10.70.6.22",
    db_port="1521",
    db_type="oracle",
    dataset_id="administracao_recursos_humanos_ergon",
    vault_secret_path="ergon-hom",
    table_parameters=ergon_queries,
)

ergon_monthly_update_schedule = Schedule(clocks=untuple(ergon_clocks))
