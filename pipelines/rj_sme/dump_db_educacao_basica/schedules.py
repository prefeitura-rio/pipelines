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
# SME Schedules
#
#####################################

sme_queries = {
    "aluno": {
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "dbt_model_secret_parameters": {"hash_seed": "hash_seed"},
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
    "aluno_historico": {
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "partition_columns": "Ano",
        "partition_date_format": "%Y",
        "materialization_mode": "prod",
        "dump_mode": "append",
        "dbt_model_secret_parameters": {"hash_seed": "hash_seed"},
        "execute_query": """
            SELECT
                *
            FROM GestaoEscolar.dbo.VW_BI_Aluno_Todos_LGPD
        """,
        "interval": timedelta(days=180),
    },
    "aluno_turma": {
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "partition_columns": "Ano",
        "partition_date_format": "%Y",
        "materialization_mode": "prod",
        "dump_mode": "append",
        "dbt_model_secret_parameters": {"hash_seed": "hash_seed"},
        "execute_query": """
            SELECT
                *
            FROM GestaoEscolar.dbo.VW_BI_Aluno_Turma
        """,
    },
    "avaliacao": {
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "partition_columns": "Ano",
        "partition_date_format": "%Y",
        "materialization_mode": "prod",
        "dump_mode": "append",
        "dbt_model_secret_parameters": {"hash_seed": "hash_seed"},
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Avaliacao",
    },
    "coc": {  # essa tabela utiliza a view coc0 pois contem o coc 0 e de 1 a 5
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "partition_columns": "Ano",
        "partition_date_format": "%Y",
        "materialization_mode": "prod",
        "dump_mode": "append",
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
    "dependencia": {
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Dependencia",
    },
    "escola": {
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
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
    "frequencia": {
        "partition_columns": "datainicio",
        "partition_date_format": "%Y-%m-%d",
        "lower_bound_date": "2022-03-01",
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,  # exceeds minimum (2022-05-31 -> 20,41GB)
        "materialization_mode": "prod",
        "dump_mode": "append",
        "dbt_model_secret_parameters": {"hash_seed": "hash_seed"},
        "execute_query": """
            SELECT
                esc_id AS esc_id,
                tur_id AS tur_id,
                turma AS turma,
                alu_id AS alu_id,
                coc AS coc,
                dataInicio AS datainicio,
                dataFim AS datafim,
                diasLetivos AS diasletivos,
                temposLetivos AS temposletivos,
                faltasGlb AS faltasglb,
                dis_id AS dis_id,
                disciplinaCodigo AS disciplinacodigo,
                disciplina AS disciplina,
                faltasDis AS faltasdis,
                cargaHorariaSemanal AS cargahorariasemanal
            FROM GestaoEscolar.dbo.VW_BI_Frequencia
        """,
    },
    "movimentacao": {
        "partition_columns": "Data_mov",
        "partition_date_format": "%Y-%m-%d",
        "lower_bound_date": "2022-03-01",
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "dbt_model_secret_parameters": {"hash_seed": "hash_seed"},
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Movimentacao_lgpd",
    },
    "turma": {
        "dataset_id": "educacao_basica_frequencia",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "execute_query": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Turma",
    },
    "turno": {
        "dataset_id": "educacao_basica_frequencia",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "execute_query": """
            SELECT
                trn_id,
                ent_id,
                ttn_id,
                trn_descricao,
                trn_padrao,
                trn_situacao,
                trn_dataCriacao,
                trn_dataAlteracao,
                trn_controleTempo,
                trn_horaInicio,
                trn_horaFim
            FROM GestaoEscolar.dbo.ACA_Turno
        """,
    },
    "curriculo_periodo": {
        "dataset_id": "educacao_basica_frequencia",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "execute_query": """
            SELECT
                cur_id,
                crr_id,
                crp_id,
                mep_id,
                crp_ordem,
                crp_descricao,
                crp_idadeIdealAnoInicio,
                crp_idadeIdealMesInicio,
                crp_idadeIdealAnoFim,
                crp_idadeIdealMesFim,
                crp_situacao,
                crp_dataCriacao,
                crp_dataAlteracao,
                crp_controleTempo,
                crp_qtdeDiasSemana,
                crp_qtdeTemposSemana,
                crp_qtdeHorasDia,
                crp_qtdeMinutosDia,
                crp_qtdeEletivasAlunos,
                crp_ciclo,
                crp_turmaAvaliacao,
                crp_nomeAvaliacao,
                crp_qtdeTemposDia,
                crp_concluiNivelEnsino,
                tci_id,
                crp_fundoFrente,
                tcp_id,
                clg_id
            FROM GestaoEscolar.dbo.ACA_CurriculoPeriodo
        """,
    },
    "tipo_turno": {
        "dataset_id": "educacao_basica_frequencia",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "execute_query": """
            SELECT
                ttn_id,
                ttn_nome,
                ttn_situacao,
                ttn_dataCriacao,
                ttn_dataAlteracao,
                ttn_tipo,
                ttn_classificacao
            FROM GestaoEscolar.dbo.ACA_TipoTurno
        """,
    },
    "turma_disciplina_rel": {
        "dataset_id": "educacao_basica_frequencia",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "execute_query": """
            SELECT
                tur_id,
                tud_id
            FROM GestaoEscolar.dbo.TUR_TurmaRelTurmaDisciplina
        """,
    },
    "turma_curriculo": {
        "dataset_id": "educacao_basica_frequencia",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "execute_query": """
            SELECT
                tur_id,
                cur_id,
                crr_id,
                crp_id,
                tcr_prioridade,
                tcr_situacao,
                tcr_dataCriacao,
                tcr_dataAlteracao
            FROM GestaoEscolar.dbo.TUR_TurmaCurriculo
        """,
    },
    "turma_aula": {
        "dataset_id": "educacao_basica_frequencia",
        "partition_columns": "tau_dataAlteracao",
        "partition_date_format": "%Y-%m-%d",
        "lower_bound_date": "current_month",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "execute_query": """
            SELECT
                tud_id,
                tau_id,
                tpc_id,
                tau_sequencia,
                tau_data,
                tau_numeroAulas,
                tau_planoAula,
                tau_diarioClasse,
                tau_situacao,
                tau_dataCriacao,
                tau_dataAlteracao,
                tau_conteudo,
                tau_efetivado,
                tau_atividadeCasa,
                tdt_posicao,
                pro_id,
                tau_sintese,
                tau_reposicao,
                usu_id,
                usu_idDocenteAlteracao,
                tau_statusFrequencia,
                tau_statusAtividadeAvaliativa,
                tau_statusAnotacoes,
                tau_statusPlanoAula,
                tau_recursosUtilizados
            FROM GestaoEscolar.dbo.CLS_TurmaAula
        """,
    },
    "turma_aula_aluno": {
        "dataset_id": "educacao_basica_frequencia",
        "partition_columns": "taa_dataAlteracao",
        "partition_date_format": "%Y-%m-%d",
        "lower_bound_date": "current_month",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "execute_query": """
            SELECT
                tud_id,
                tau_id,
                alu_id,
                mtu_id,
                mtd_id,
                taa_frequencia,
                taa_situacao,
                taa_dataCriacao,
                taa_dataAlteracao,
                taa_anotacao,
                taa_frequenciaBitMap,
                usu_idDocenteAlteracao
            FROM GestaoEscolar.dbo.CLS_TurmaAulaAluno
        """,
    },
    "turma_disciplina": {
        "dataset_id": "educacao_basica_frequencia",
        "partition_columns": "tud_dataAlteracao",
        "partition_date_format": "%Y-%m-%d",
        "lower_bound_date": "current_month",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "prod",
        "execute_query": """
            SELECT
                tud_id,
                tud_codigo,
                tud_nome,
                tud_multiseriado,
                tud_vagas,
                tud_minimoMatriculados,
                tud_duracao,
                tud_modo,
                tud_tipo,
                tud_dataInicio,
                tud_dataFim,
                tud_situacao,
                tud_dataCriacao,
                tud_dataAlteracao,
                tud_cargaHorariaSemanal,
                tud_aulaForaPeriodoNormal,
                tud_global,
                tud_disciplinaEspecial,
                tud_naoLancarNota,
                tud_naoLancarFrequencia,
                tud_naoExibirNota,
                tud_naoExibirFrequencia,
                tud_semProfessor,
                tud_naoExibirBoletim
            FROM GestaoEscolar.dbo.TUR_TurmaDisciplina
        """,
    },
}


sme_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1, 2, 10, tzinfo=pytz.timezone("America/Sao_Paulo")),
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
