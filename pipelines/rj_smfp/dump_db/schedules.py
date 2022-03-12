"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants
from pipelines.utils.utils import query_to_line, untuple_clocks as untuple


#####################################
#
# Ergon Schedules
#
#####################################

ergon_queries = {
    "cargo": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_CARGOS_",
    },
    "categoria": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_CATEGORIAS_",
    },
    "empresa": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_EMPRESAS",
    },
    "matricula": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_ERG_MATRICULAS",
    },
    # TODO check if administracao_recursos_humanos_folha_salarial.fita_banco works in bq and storage
    "fita_banco": {
        "partition_column": "MES_ANO",
        "lower_bound_date": None,
        "dump_type": "append",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_FITA_BANCO",
    },
    # TODO check if administracao_recursos_humanos_folha_salarial.folha_empresa works in bq and storage
    "folha_empresa": {
        "partition_column": "MES_ANO",
        "lower_bound_date": None,
        "dump_type": "append",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_FOLHAS_EMP",
    },
    "forma_prov": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_FORMAS_PROV_",
    },
    "funcionario": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_FUNCIONARIOS",
    },
    "horario_trabalho": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_HORARIO_TRAB_",
    },
    "h_setor": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_HSETOR_",
    },
    "jornada": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_JORNADAS_",
    },
    "orgaos_externos": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_ORGAOS_EXTERNOS",
    },
    "orgaos_regime_juridico": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_ORGAOS_REGIMES_JUR_",
    },
    "provimentos_ev": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_PROVIMENTOS_EV",
    },
    "regime_juridico": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_REGIMES_JUR_",
    },
    "tipo_folha": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_TIPO_FOLHA",
    },
    "tipo_orgao": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_TIPO_ORGAO",
    },
    "tipo_vinculo": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_TIPO_VINC_",
    },
    "vinculo": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROMC_ERGON.VW_DLK_ERG_VINCULOS",
    },
}

ergon_clocks = [
    IntervalClock(
        interval=timedelta(days=30),
        start_date=datetime(
            2022, 3, 12, 3, 0, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=3 * count),
        labels=[
            constants.RJ_SMFP_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "batch_size": 50000,
            "vault_secret_path": "ergon-hom",
            "db_database": "P01.PCRJ",
            "db_host": "10.70.6.22",
            "db_port": "1521",
            "db_type": "oracle",
            "dataset_id": "administracao_recursos_humanos_folha_salarial",
            "table_id": table_id,
            "dump_type": parameters["dump_type"],
            "partition_column": parameters["partition_column"],
            "lower_bound_date": parameters["lower_bound_date"],
            "execute_query": query_to_line(parameters["execute_query"]),
        },
    )
    for count, (table_id, parameters) in enumerate(ergon_queries.items())
]

ergon_monthly_update_schedule = Schedule(clocks=untuple(ergon_clocks))
