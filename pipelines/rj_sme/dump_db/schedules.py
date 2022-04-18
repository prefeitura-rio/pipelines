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
    "turma": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Turma",
    },
    "dependencia": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Dependencia",
    },
    "avaliacao": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Avaliacao",
    },
    "aluno_turma_coc": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": """
            SELECT 
                Ano AS Ano,
                CRE AS CRE,
                Unidade AS Unidade,
                Grupamento AS Grupamento,
                Turma AS Turma,
                Turno AS Turno,
                COC AS COC,
                Turmas AS Turmas,
                Alunos AS Alunos,
                Masculinos AS Masculinos,
                Femininos AS Femininos,
                Não_Def AS Nao_Def,
                Def AS Def,
                Masculinos_Não_Def AS Masculinos_Nao_Def,
                Masculinos_Def AS Masculinos_Def,
                Femininos_Não_Def AS Femininos_Nao_Def,
                Femininos_Def AS Femininos_Def,
                Vagas AS Vagas,
                capacidade AS capacidade,
                tur_id AS tur_id,
                pft_capacidade AS pft_capacidade,
            FROM GestaoEscolar.dbo.VW_BI_Aluno_Turma_COC
        """,
    },
    "aluno_tuma_coc0": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": """
            SELECT 
                Ano AS Ano,
                CRE AS CRE,
                Unidade AS Unidade,
                Grupamento AS Grupamento,
                Turma AS Turma,
                Turno AS Turno,
                COC AS COC,
                Turmas AS Turmas,
                Alunos AS Alunos,
                Masculinos AS Masculinos,
                Femininos AS Femininos,
                Não_Def AS Nao_Def,
                Def AS Def,
                Masculinos_Não_Def AS Masculinos_Nao_Def,
                Masculinos_Def AS Masculinos_Def,
                Femininos_Não_Def AS Femininos_Nao_Def,
                Femininos_Def AS Femininos_Def,
                Vagas AS Vagas,
                capacidade AS capacidade,
                tur_id AS tur_id,
                pft_capacidade AS pft_capacidade,
            FROM GestaoEscolar.dbo.VW_BI_Aluno_Turma_com_COC0
        """,
    },
    "frequencia": {
        "partition_column": "dataInicio",
        "lower_bound_date": None,
        "dump_type": "append",
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Frequencia",
    },
    "escola": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": """
            SELECT 
                CRE,
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
    },
    "aluno": {
        "partition_column": None,
        "lower_bound_date": None,
        "dump_type": "overwrite",
        "execute_query": """
            SELECT 
                Ano,
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
    },
}
sme_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=15 * count),
        labels=[
            constants.RJ_SME_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "batch_size": 50000,
            "vault_secret_path": "clustersqlsme",
            "db_database": "GestaoEscolar",
            "db_host": "10.70.6.103",
            "db_port": "1433",
            "db_type": "sql_server",
            "dataset_id": "educacao_basica",
            "table_id": table_id,
            "dump_type": parameters["dump_type"],
            "partition_column": parameters["partition_column"],
            "lower_bound_date": parameters["lower_bound_date"],
            "execute_query": query_to_line(parameters["execute_query"]),
        },
    )
    for count, (table_id, parameters) in enumerate(sme_queries.items())
]

sme_educacao_basica_daily_update_schedule = Schedule(clocks=untuple(sme_clocks))
