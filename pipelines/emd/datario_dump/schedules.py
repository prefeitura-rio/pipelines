"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants
from pipelines.utils import query_to_line, untuple_clocks as untuple


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


#####################################
#
# SME Schedules
#
#####################################
sme_queries = {
    "turma": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Turma",
    "dependencia": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Dependencia",
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

sme_daily_update_schedule = Schedule(clocks=untuple(sme_clocks))


#####################################
#
# 1746 Schedules
#
#####################################
_1746_queries = {
    "chamado": """
        USE REPLICA1746
        SELECT
            DISTINCT ch.id_chamado,
            CONVERT (
                VARCHAR,
                CONVERT(DATETIME, ch.dt_inicio, 10),
                20
            ) AS [dt_inicio],
            CONVERT (VARCHAR, CONVERT(DATETIME, ch.dt_fim, 10), 20) AS [dt_fim],
            tr.id_territorialidade,
            tr.no_area_planejamento,
            id_unidade_organizacional,
            no_unidade_organizacional,
            uo.fl_ouvidoria,
            id_tipo,
            no_tipo,
            id_subtipo,
            no_subtipo,
            no_status,
            id_bairro,
            rtrim(ltrim(no_bairro)) AS 'no_bairro',
            ch.nu_coord_x,
            ch.nu_coord_y,
            id_logradouro,
            no_logradouro,
            ch.ds_endereco_numero,
            no_categoria,
            ccs.ic_prazo_tipo,
            ccs.ic_prazo_unidade_tempo,
            ccs.nu_prazo,
            chs.dt_alvo_finalizacao,
            chs.dt_alvo_diagnostico,
            cl.dt_real_diagnostico
        FROM
            tb_chamado AS ch
            INNER JOIN (
                SELECT
                    max (id_classificacao_chamado) Ultima_Classificacao,
                    id_chamado_fk
                FROM
                    tb_classificacao_chamado
                GROUP BY
                    id_chamado_fk
            ) AS cch ON cch.id_chamado_fk = ch.id_chamado
            INNER JOIN tb_classificacao_chamado AS cl ON cl.id_classificacao_chamado = Ultima_Classificacao
            INNER JOIN tb_classificacao AS cll ON cll.id_classificacao = cl.id_classificacao_fk
            INNER JOIN tb_subtipo AS sub ON sub.id_subtipo = cll.id_subtipo_fk
            INNER JOIN tb_tipo AS tp ON tp.id_tipo = sub.id_tipo_fk
            INNER JOIN tb_categoria AS ct ON ct.id_categoria = ch.id_categoria_fk
            LEFT JOIN (
                SELECT
                    max(id_andamento) Ultimo_Status,
                    id_chamado_fk
                FROM
                    tb_andamento
                GROUP BY
                    id_chamado_fk
            ) AS ad ON ad.id_chamado_fk = ch.id_chamado
            LEFT JOIN tb_andamento AS an ON an.id_andamento = ad.Ultimo_Status
            LEFT JOIN tb_status_especifico AS ste ON ste.id_status_especifico = an.id_status_especifico_fk
            INNER JOIN tb_status AS st ON st.id_status = ch.id_status_fk
            INNER JOIN (
                SELECT
                    max(id_responsavel_chamado) Responsavel,
                    id_chamado_fk
                FROM
                    tb_responsavel_chamado
                GROUP BY
                    id_chamado_fk
            ) AS rc ON rc.id_chamado_fk = ch.id_chamado
            INNER JOIN tb_responsavel_chamado AS rec ON rec.id_responsavel_chamado = rc.Responsavel
            INNER JOIN tb_unidade_organizacional AS uo ON uo.id_unidade_organizacional = rec.id_unidade_organizacional_fk
            INNER JOIN (
                SELECT
                    max (id_protocolo_chamado) primeiro_protocolo,
                    id_chamado_fk
                FROM
                    tb_protocolo_chamado
                GROUP BY
                    id_chamado_fk
            ) AS prc ON prc.id_chamado_fk = ch.id_chamado
            INNER JOIN tb_protocolo_chamado AS prcc ON prcc.id_protocolo_chamado = prc.primeiro_protocolo
            INNER JOIN tb_protocolo AS pr ON pr.id_protocolo = prcc.id_protocolo_fk
            LEFT JOIN tb_pessoa AS pe ON pe.id_pessoa = ch.id_pessoa_fk
            INNER JOIN tb_origem_ocorrencia AS oc ON oc.id_origem_ocorrencia = ch.id_origem_ocorrencia_fk
            LEFT JOIN tb_bairro_logradouro AS bl ON bl.id_bairro_logradouro = ch.id_bairro_logradouro_fk
            LEFT JOIN tb_logradouro AS lg ON lg.id_logradouro = bl.id_logradouro_fk
            LEFT JOIN tb_bairro AS br ON br.id_bairro = bl.id_bairro_fk
            LEFT JOIN tb_classificacao_cenario_sla AS ccs ON ccs.id_classificacao_fk = cl.id_classificacao_fk
            LEFT JOIN tb_justificativa AS jt ON jt.id_justificativa = cl.id_justificativa_fk
            LEFT JOIN tb_chamado_sla AS chs ON chs.id_chamado_fk = ch.id_chamado
            LEFT JOIN tb_territorialidade_regiao_administrativa_bairro AS tra ON tra.id_bairro_fk = br.id_bairro
            LEFT JOIN tb_territorialidade_regiao_administrativa AS trg ON trg.id_territorialidade_regiao_administrativa = tra.id_territorialidade_regiao_administrativa_fk
            LEFT JOIN tb_territorialidade AS tr ON tr.id_territorialidade = trg.id_territorialidade_fk
        ORDER BY
            ch.id_chamado ASC
    """,
}
_1746_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=3 * count),
        labels=[
            constants.EMD_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "batch_size": 50000,
            "dataset_id": "administracao_servicos_publicos_1746",
            "db_database": "REPLICA1746",
            "db_host": "10.70.1.34",
            "db_port": "1433",
            "db_type": "sql_server",
            "dump_type": "overwrite",
            "execute_query": query_to_line(execute_query),
            "table_id": table_id,
            "vault_secret_path": "clustersql2",
        },
    )
    for count, (table_id, execute_query) in enumerate(_1746_queries.items())
]

_1746_daily_update_schedule = Schedule(clocks=untuple(_1746_clocks))
