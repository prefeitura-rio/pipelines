# -*- coding: utf-8 -*-
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
# SME Schedules
#
#####################################
sme_queries = {
    "turma": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Turma",
    },
    "dependencia": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Dependencia",
    },
    "avaliacao": {
        "dump_type": "overwrite",
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Avaliacao",
    },
    "aluno_turma_coc": {
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
                pft_capacidade AS pft_capacidade
            FROM GestaoEscolar.dbo.VW_BI_Aluno_Turma_COC
        """,
    },
    "aluno_tuma_coc0": {
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
                pft_capacidade AS pft_capacidade
            FROM GestaoEscolar.dbo.VW_BI_Aluno_Turma_com_COC0
        """,
    },
    "frequencia": {
        "partition_column": "dataInicio",
        "lower_bound_date": "2022-03-01",
        "dump_type": "append",
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Frequencia",
    },
    "escola": {
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

sme_clocks = generate_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SME_AGENT_LABEL.value,
    ],
    db_database="GestaoEscolar",
    db_host="10.70.6.103",
    db_port="1433",
    db_type="sql_server",
    dataset_id="educacao_basica",
    vault_secret_path="clustersqlsme",
    table_parameters=sme_queries,
)

sme_educacao_basica_daily_update_schedule = Schedule(clocks=untuple(sme_clocks))
