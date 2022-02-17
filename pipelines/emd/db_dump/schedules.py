"""
Schedules for the database dump pipeline
"""


from calendar import month
from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants

untuple = lambda clocks: [
    clock[0] if type(clock) == tuple else clock for clock in clocks
]


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
            "execute_query": execute_query,
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
    "escola": "SELECT      CRE,     Designacao,     Denominacao,     Endereco,     Bairro,     CEP,     eMail,     Telefone,     Direçao as Direcao,     MicroArea,     Polo,     Tipo_Unidade,     INEP,     SICI,     Salas_Recurso,     Salas_Aula,     Salas_Aula_Utilizadas,     Tot_Escola,     esc_id FROM GestaoEscolar.dbo.VW_BI_Escola",
    "turma": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Turma",
    "dependencia": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Dependencia",
    ### POR ENQUANTO frequencia USA DADOS DE 2019, NAO PRECISA ATUALIZAR DIARIAMENTE
    # "frequencia": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Frequencia",
    "aluno": "SELECT * FROM GestaoEscolar.dbo.VW_BI_Aluno_lgpd",
}
sme_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=3 * count),
        labels=[
            constants.EMD_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "batch_size": 50000,
            "dataset_id": "educacao_basica",
            "db_database": "GestaoEscolar",
            "db_host": "10.70.6.103",
            "db_port": "1433",
            "db_type": "sql_server",
            "dump_type": "overwrite",
            "execute_query": execute_query,
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
                select distinct ch.id_chamado, CONVERT (VARCHAR, CONVERT(DATETIME, ch.dt_inicio, 10)
                , 20) AS [dt_inicio], CONVERT (VARCHAR, CONVERT(DATETIME, ch.dt_fim, 10), 20) AS
                [dt_fim], tr.id_territorialidade, tr.no_area_planejamento, id_unidade_organizacional
                , no_unidade_organizacional, uo.fl_ouvidoria, id_tipo, no_tipo, id_subtipo,
                no_subtipo, no_status, id_bairro, rtrim(ltrim(no_bairro)) as 'no_bairro',
                ch.nu_coord_x, ch.nu_coord_y, id_logradouro, no_logradouro, ch.ds_endereco_numero,
                no_categoria, ccs.ic_prazo_tipo, ccs.ic_prazo_unidade_tempo, ccs.nu_prazo,
                chs.dt_alvo_finalizacao, chs.dt_alvo_diagnostico, cl.dt_real_diagnostico from
                tb_chamado as ch inner join (select max (id_classificacao_chamado)
                Ultima_Classificacao, id_chamado_fk from tb_classificacao_chamado group by
                id_chamado_fk) as cch on cch.id_chamado_fk = ch.id_chamado inner join
                tb_classificacao_chamado as cl on cl.id_classificacao_chamado = Ultima_Classificacao
                inner join tb_classificacao as cll on cll.id_classificacao = cl.id_classificacao_fk
                inner join tb_subtipo as sub on sub.id_subtipo = cll.id_subtipo_fk inner join
                tb_tipo as tp on tp.id_tipo = sub.id_tipo_fk inner join tb_categoria as ct on
                ct.id_categoria = ch.id_categoria_fk left join (select max(id_andamento)
                Ultimo_Status,id_chamado_fk from tb_andamento group by id_chamado_fk ) as ad on
                ad.id_chamado_fk = ch.id_chamado left join tb_andamento as an on an.id_andamento =
                ad.Ultimo_Status left join tb_status_especifico as ste on ste.id_status_especifico =
                an.id_status_especifico_fk inner join tb_status as st on st.id_status =
                ch.id_status_fk inner join (select max(id_responsavel_chamado) Responsavel,
                id_chamado_fk from tb_responsavel_chamado group by id_chamado_fk) as rc on
                rc.id_chamado_fk = ch.id_chamado inner join tb_responsavel_chamado as rec on
                rec.id_responsavel_chamado = rc.Responsavel inner join tb_unidade_organizacional as
                uo on uo.id_unidade_organizacional = rec.id_unidade_organizacional_fk inner join
                (select max (id_protocolo_chamado) primeiro_protocolo, id_chamado_fk from
                tb_protocolo_chamado group by id_chamado_fk) as prc on prc.id_chamado_fk =
                ch.id_chamado  inner join tb_protocolo_chamado as prcc on prcc.id_protocolo_chamado
                = prc.primeiro_protocolo inner join tb_protocolo as pr on pr.id_protocolo =
                prcc.id_protocolo_fk left join tb_pessoa as pe on pe.id_pessoa = ch.id_pessoa_fk 
                inner join tb_origem_ocorrencia as oc on oc.id_origem_ocorrencia =
                ch.id_origem_ocorrencia_fk left join tb_bairro_logradouro as bl on
                bl.id_bairro_logradouro = ch.id_bairro_logradouro_fk left join tb_logradouro as lg
                on lg.id_logradouro = bl.id_logradouro_fk left join tb_bairro as br on br.id_bairro
                = bl.id_bairro_fk left join tb_classificacao_cenario_sla as ccs on
                ccs.id_classificacao_fk = cl.id_classificacao_fk left join tb_justificativa as jt on
                jt.id_justificativa = cl.id_justificativa_fk left join tb_chamado_sla as chs on
                chs.id_chamado_fk = ch.id_chamado left join
                tb_territorialidade_regiao_administrativa_bairro as tra on tra.id_bairro_fk =
                br.id_bairro left join tb_territorialidade_regiao_administrativa as trg on
                trg.id_territorialidade_regiao_administrativa =
                tra.id_territorialidade_regiao_administrativa_fk left join tb_territorialidade as tr
                on tr.id_territorialidade = trg.id_territorialidade_fk order by ch.id_chamado asc
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
            "execute_query": execute_query,
            "table_id": table_id,
            "vault_secret_path": "clustersql2",
        },
    )
    for count, (table_id, execute_query) in enumerate(_1746_queries.items())
]

_1746_daily_update_schedule = Schedule(clocks=untuple(_1746_clocks))
