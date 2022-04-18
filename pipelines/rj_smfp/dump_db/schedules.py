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
    "cargo": "SELECT * FROM C_ERGON.VW_DLK_ERG_CARGOS_",
    "categoria": "SELECT * FROMC_ERGON.VW_DLK_ERG_CATEGORIAS_",
    "empresa": "SELECT * FROMC_ERGON.VW_DLK_ERG_EMPRESAS",
    "matricula": "SELECT * FROMC_ERGON.VW_DLK_ERG_ERG_MATRICULAS",
    "fita_banco": "SELECT * FROMC_ERGON.VW_DLK_ERG_FITA_BANCO",
    "folha_empresa": "SELECT * FROMC_ERGON.VW_DLK_ERG_FOLHAS_EMP",
    "forma_prov": "SELECT * FROMC_ERGON.VW_DLK_ERG_FORMAS_PROV_",
    "funcionario": "SELECT * FROMC_ERGON.VW_DLK_ERG_FUNCIONARIOS",
    "horario_trabalho": "SELECT * FROMC_ERGON.VW_DLK_ERG_HORARIO_TRAB_",
    "h_setor": "SELECT * FROMC_ERGON.VW_DLK_ERG_HSETOR_",
    "jornada": "SELECT * FROMC_ERGON.VW_DLK_ERG_JORNADAS_",
    "orgaos_externos": "SELECT * FROMC_ERGON.VW_DLK_ERG_ORGAOS_EXTERNOS",
    "orgaos_regime_juridico": "SELECT * FROMC_ERGON.VW_DLK_ERG_ORGAOS_REGIMES_JUR_",
    "provimentos_ev": "SELECT * FROMC_ERGON.VW_DLK_ERG_PROVIMENTOS_EV",
    "regime_juridico": "SELECT * FROMC_ERGON.VW_DLK_ERG_REGIMES_JUR_",
    "tipo_folha": "SELECT * FROMC_ERGON.VW_DLK_ERG_TIPO_FOLHA",
    "tipo_orgao": "SELECT * FROMC_ERGON.VW_DLK_ERG_TIPO_ORGAO",
    "tipo_vinculo": "SELECT * FROMC_ERGON.VW_DLK_ERG_TIPO_VINC_",
    "vinculo": "SELECT * FROMC_ERGON.VW_DLK_ERG_VINCULOS",
}

ergon_clocks = [
    IntervalClock(
        interval=timedelta(days=30),
        start_date=datetime(2022, 3, 5, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=3 * count),
        labels=[
            constants.EMD_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "batch_size": 50000,
            "dataset_id": "administracao_recursos_humanos_folha_salarial",
            "db_database": "P01.PCRJ",
            "db_host": "10.70.6.22",
            "db_port": "1521",
            "db_type": "oracle",
            "dump_type": "overwrite",
            "execute_query": query_to_line(execute_query),
            "table_id": table_id,
            "vault_secret_path": "ergon-hom",
        },
    )
    for count, (execute_query, table_id) in enumerate(ergon_queries.items())
]

ergon_monthly_update_schedule = Schedule(clocks=untuple(ergon_clocks))
