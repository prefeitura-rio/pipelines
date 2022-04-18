"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz

from pipelines.constants import constants
from pipelines.utils.dump_db.utils import generate_schedules
from pipelines.utils.utils import untuple_clocks as untuple


#####################################
#
# Ergon Schedules
#
#####################################

ergon_queries = {
    "cargo": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_CARGOS_",
    },
    "categoria": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_CATEGORIAS_",
    },
    "empresa": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_EMPRESAS",
    },
    "matricula": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ERG_MATRICULAS",
    },
    # TODO check if administracao_recursos_humanos_folha_salarial.fita_banco works in bq and storage
    "fita_banco": {
        "partition_column": "MES_ANO",
        "dump_type": "append",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FITA_BANCO",
    },
    # TODO check if administracao_recursos_humanos_folha_salarial.folha_empresa works in bq and storage
    "folha_empresa": {
        "partition_column": "MES_ANO",
        "dump_type": "append",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FOLHAS_EMP",
    },
    "forma_prov": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FORMAS_PROV_",
    },
    "funcionario": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FUNCIONARIOS",
    },
    "horario_trabalho": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_HORARIO_TRAB_",
    },
    "h_setor": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_HSETOR_",
    },
    "jornada": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_JORNADAS_",
    },
    "orgaos_externos": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ORGAOS_EXTERNOS",
    },
    "orgaos_regime_juridico": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ORGAOS_REGIMES_JUR_",
    },
    "provimentos_ev": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_PROVIMENTOS_EV",
    },
    "regime_juridico": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_REGIMES_JUR_",
    },
    "tipo_folha": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_FOLHA",
    },
    "tipo_orgao": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_ORGAO",
    },
    "tipo_vinculo": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_VINC_",
    },
    "vinculo": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_VINCULOS",
    },
}


ergon_clocks = generate_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2022, 3, 12, 3, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    db_database="P01.PCRJ",
    db_host="10.70.6.22",
    db_port="1521",
    db_type="oracle",
    dataset_id="administracao_recursos_humanos_folha_salarial",
    vault_secret_path="ergon-hom",
    table_parameters=ergon_queries,
)

ergon_monthly_update_schedule = Schedule(clocks=untuple(ergon_clocks))
