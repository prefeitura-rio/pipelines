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
# SME Schedules
#
#####################################
sme_queries = {
    "turma": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Turma",
    "dependencia": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Dependencia",
    "avaliacao": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Avaliacao",
    "aluno_turma_coc": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Aluno_Turma_COC",
    "aluno_tuma_coc0": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Aluno_Turma_com_COC0",
    # POR ENQUANTO frequencia USA DADOS DE 2019, NAO PRECISA ATUALIZAR DIARIAMENTE
    # "frequencia": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Frequencia",
    "escola": """
        SELECT CRE,
            Designacao,
            Denominacao,
            Endereco,
            Bairro,
            CEP,
            eMail,
            Telefone,
            Direçao as Direcao,
            MicroArea,
            Polo,
            Tipo_Unidade,
            INEP,
            SICI,
            Salas_Recurso,
            Salas_Aula,
            Salas_Aula_Utilizadas,
            Tot_Escola,
            esc_id
        FROM GestaoEscolar.dbo.VW_BI_Escola
    """,
    "aluno": """
        SELECT Ano,
            Matricula,
            Nome,
            Sexo,
            Endereco,
            Bairro,
            CEP,
            Filiacao_1,
            Filiacao_2,
            Mora_com_Filiacao,
            CPF,
            NIS_Aluno,
            NIS_Resp,
            Raça_Cor as Raca_Cor,
            Cod_def,
            Deficiência as Deficiencia,
            Tipo_Transporte,
            Bolsa_Familia,
            CFC,
            Territorios_Sociais,
            Clube_Escolar,
            Nucleo_Artes,
            Mais_Educacao,
            DataNascimento,
            Idade_Atual,
            Idade_3112,
            Turma,
            UP_Aval,
            Situacao,
            Cod_Ult_Mov,
            Ult_Movimentação as Ult_Movimentacao,
            Tot_Aluno,
            alu_id,
            tur_id
        FROM GestaoEscolar.dbo.VW_BI_Aluno_lgpd
    """,
}
sme_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=3 * count),
        labels=[
            constants.RJ_SME_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "batch_size": 50000,
            "dataset_id": "educacao_basica",
            "db_database": "GestaoEscolar",
            "db_host": "10.70.6.103",
            "db_port": "1433",
            "db_type": "sql_server",
            "dump_type": "overwrite",
            "execute_query": query_to_line(execute_query),
            "table_id": table_id,
            "vault_secret_path": "clustersqlsme",
        },
    )
    for count, (table_id, execute_query) in enumerate(sme_queries.items())
]

sme_educacao_basica_daily_update_schedule = Schedule(clocks=untuple(sme_clocks))
