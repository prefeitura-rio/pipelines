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
# 1746 Schedules
#
#####################################


QUERY_CHAMADO_1746_DS = """
select
    distinct ch.id_chamado,
    ch.ds_chamado,
    CONVERT (
        VARCHAR,
        CONVERT(DATETIME, ch.dt_inicio, 10),
        20
    ) AS [dt_inicio],
    CONVERT (
        VARCHAR,
        CONVERT(DATETIME, ch.dt_fim, 10),
        20
    ) AS [dt_fim],
    tr.id_territorialidade,
    tr.no_area_planejamento,
    uo.id_unidade_organizacional,
    uo.no_unidade_organizacional,
    CASE
        WHEN vuo.NO_N2 = '' THEN NULL
        WHEN vuo.NO_N1 IS NOT NULL AND vuo.NO_N1 <> ins.no_instituicao THEN vuo.NO_N1
        when uop.id_unidade_pai_fk in (71, 610, 55, 23) then uop2.no_unidade_organizacional
        WHEN uop.no_unidade_organizacional <> ins.no_instituicao THEN uop.no_unidade_organizacional
        WHEN uop.no_unidade_organizacional = ins.no_instituicao THEN uo.no_unidade_organizacional
    ELSE NULL END AS [uo_mae],
    case
        when ch.dt_fim is null then 'Não Encerrado'
        when ch.dt_fim is not null then 'Encerrado'
    end as 'situacao',
    case
        when st.no_status = 'Fechado com solução' then 'Atendido'
        when st.no_status in (
            'Fechado com providências', 'Fechado com informação'
        ) then 'Atendido parcialmente'
        when st.no_status in (
            'Sem possibilidade de atendimento', 'Cancelado'
        ) then 'Não atendido'
        when st.no_status = 'Não constatado' then 'Não constatado'
    else 'Andamento' end as 'tipo_situacao',
    case when CONVERT (
        VARCHAR,
        CONVERT(
        DATETIME, chs.dt_alvo_finalizacao,
        10
        ),
        20
    ) >= CONVERT (
        VARCHAR,
        CONVERT(DATETIME, ch.dt_fim, 10),
        20
    ) then 'No prazo' when CONVERT (
        VARCHAR,
        CONVERT(
        DATETIME, chs.dt_alvo_finalizacao,
        10
        ),
        20
    ) < CONVERT (
        VARCHAR,
        CONVERT(DATETIME, ch.dt_fim, 10),
        20
    ) then 'Fora do prazo' else 'Fora do prazo' end as 'prazo',
    uo.fl_ouvidoria,
    id_tipo,
    no_tipo,
    id_subtipo,
    no_subtipo,
    no_status,
    id_bairro,
    rtrim(
        ltrim(no_bairro)
    ) as 'no_bairro',
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
    cl.dt_real_diagnostico,
    count (
        case when cv.ic_vinculo = 'O'
        or cv.ic_vinculo = 'S' then cv.id_chamado_pai_fk end
    ) as 'reclamacoes',
    no_justificativa,
    oc.id_origem_ocorrencia
from
    tb_chamado as ch
    inner join (
        select
        max (id_classificacao_chamado) Ultima_Classificacao,
        id_chamado_fk
        from
        tb_classificacao_chamado
        group by
        id_chamado_fk
    ) as cch on cch.id_chamado_fk = ch.id_chamado
    inner join tb_classificacao_chamado as cl on cl.id_classificacao_chamado = Ultima_Classificacao
    inner join tb_classificacao as cll on cll.id_classificacao = cl.id_classificacao_fk
    inner join tb_subtipo as sub on sub.id_subtipo = cll.id_subtipo_fk
    inner join tb_tipo as tp on tp.id_tipo = sub.id_tipo_fk
    inner join tb_categoria as ct on ct.id_categoria = ch.id_categoria_fk
    left join (
        select
        max(id_andamento) Ultimo_Status,
        id_chamado_fk
        from
        tb_andamento
        group by
        id_chamado_fk
    ) as ad on ad.id_chamado_fk = ch.id_chamado
    left join tb_andamento as an on an.id_andamento = ad.Ultimo_Status
    left join tb_status_especifico as ste on ste.id_status_especifico = an.id_status_especifico_fk
    inner join tb_status as st on st.id_status = ch.id_status_fk
    inner join (
        select
        max(id_responsavel_chamado) Responsavel,
        id_chamado_fk
        from
        tb_responsavel_chamado
        group by
        id_chamado_fk
    ) as rc on rc.id_chamado_fk = ch.id_chamado
    inner join tb_responsavel_chamado as rec on rec.id_responsavel_chamado = rc.Responsavel
    inner join tb_unidade_organizacional as uo on
        uo.id_unidade_organizacional = rec.id_unidade_organizacional_fk
    inner join (
        select
        max (id_protocolo_chamado) primeiro_protocolo,
        id_chamado_fk
        from
        tb_protocolo_chamado
        group by
        id_chamado_fk
    ) as prc on prc.id_chamado_fk = ch.id_chamado
    inner join tb_protocolo_chamado as prcc on
        prcc.id_protocolo_chamado = prc.primeiro_protocolo
    inner join tb_protocolo as pr on pr.id_protocolo = prcc.id_protocolo_fk
    left join tb_pessoa as pe on pe.id_pessoa = ch.id_pessoa_fk
    inner join tb_origem_ocorrencia as oc on
        oc.id_origem_ocorrencia = ch.id_origem_ocorrencia_fk
    left join tb_bairro_logradouro as bl on
        bl.id_bairro_logradouro = ch.id_bairro_logradouro_fk
    left join tb_logradouro as lg on lg.id_logradouro = bl.id_logradouro_fk
    left join tb_bairro as br on br.id_bairro = bl.id_bairro_fk
    left join tb_chamado_vinculado as cv on cv.id_chamado_pai_fk = ch.id_chamado
    left join tb_classificacao_cenario_sla as ccs on
        ccs.id_classificacao_fk = cl.id_classificacao_fk
    left join tb_justificativa as jt on jt.id_justificativa = cl.id_justificativa_fk
    left join tb_chamado_sla as chs on chs.id_chamado_fk = ch.id_chamado
    left join tb_territorialidade_regiao_administrativa_bairro as tra on
        tra.id_bairro_fk = br.id_bairro
    left join tb_territorialidade_regiao_administrativa as trg on
    trg.id_territorialidade_regiao_administrativa=tra.id_territorialidade_regiao_administrativa_fk
    left join tb_territorialidade as tr on
        tr.id_territorialidade = trg.id_territorialidade_fk
    left join tb_unidade_organizacional as uu on
        uu.id_unidade_organizacional = an.id_unidade_organizacional_fk
    left JOIN tb_instituicao AS ins ON ins.id_instituicao = uo.id_instituicao_fk
    LEFT join tb_unidade_organizacional as uop on
        uop.id_unidade_organizacional = uo.id_unidade_pai_fk
    LEFT join tb_unidade_organizacional as uop2 on
        uop2.id_unidade_organizacional = uop.id_unidade_pai_fk
    LEFT JOIN vw_arvore_uo AS vuo ON
        uo.id_unidade_organizacional = CASE
                                        WHEN vuo.ID_N9 <> 0 THEN vuo.ID_N9
                                        WHEN vuo.ID_N8 <> 0 THEN vuo.ID_N8
                                        WHEN vuo.ID_N7 <> 0 THEN vuo.ID_N7
                                        WHEN vuo.ID_N6 <> 0 THEN vuo.ID_N6
                                        WHEN vuo.ID_N5 <> 0 THEN vuo.ID_N5
                                        WHEN vuo.ID_N4 <> 0 THEN vuo.ID_N4
                                        WHEN vuo.ID_N3 <> 0 THEN vuo.ID_N3
                                        WHEN vuo.ID_N2 <> 0 THEN vuo.ID_N2
                                        WHEN vuo.ID_N1 <> 0 THEN vuo.ID_N1
                                        WHEN vuo.ID_N0 <> 0 THEN vuo.ID_N0 END
where
    uo.id_instituicao_fk = 3
    and id_categoria in (2)
group by
    ch.id_chamado,
    ch.ds_chamado,
    CONVERT (
        VARCHAR,
        CONVERT(DATETIME, ch.dt_inicio, 10),
        20
    ),
    CONVERT (
        VARCHAR,
        CONVERT(DATETIME, ch.dt_fim, 10),
        20
    ),
    tr.id_territorialidade,
    tr.no_area_planejamento,
    uo.id_unidade_organizacional,
    uo.no_unidade_organizacional,
    CASE
        WHEN vuo.NO_N2 = '' THEN NULL
        WHEN vuo.NO_N1 IS NOT NULL AND vuo.NO_N1 <> ins.no_instituicao THEN vuo.NO_N1
        when uop.id_unidade_pai_fk in (71, 610, 55, 23) then uop2.no_unidade_organizacional
        WHEN uop.no_unidade_organizacional <> ins.no_instituicao THEN uop.no_unidade_organizacional
        WHEN uop.no_unidade_organizacional = ins.no_instituicao THEN uo.no_unidade_organizacional
        ELSE NULL END,
    case
        when ch.dt_fim is null then 'Não Encerrado'
        when ch.dt_fim is not null then 'Encerrado'
    end,
    case
        when st.no_status = 'Fechado com solução' then 'Atendido'
        when st.no_status in (
            'Fechado com providências', 'Fechado com informação'
        ) then 'Atendido parcialmente'
        when st.no_status in (
            'Sem possibilidade de atendimento', 'Cancelado'
        ) then 'Não atendido'
        when st.no_status = 'Não constatado' then 'Não constatado'
    else 'Andamento' end,
    case when CONVERT (
        VARCHAR,
        CONVERT(
        DATETIME, chs.dt_alvo_finalizacao,
        10
        ),
        20
    ) >= CONVERT (
        VARCHAR,
        CONVERT(DATETIME, ch.dt_fim, 10),
        20
    ) then 'No prazo' when CONVERT (
        VARCHAR,
        CONVERT(
        DATETIME, chs.dt_alvo_finalizacao,
        10
        ),
        20
    ) < CONVERT (
        VARCHAR,
        CONVERT(DATETIME, ch.dt_fim, 10),
        20
    ) then 'Fora do prazo' else 'Fora do prazo' end,
    uo.fl_ouvidoria,
    id_tipo,
    no_tipo,
    id_subtipo,
    no_subtipo,
    no_status,
    id_bairro,
    rtrim(
        ltrim(no_bairro)
    ),
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
    cl.dt_real_diagnostico,
    no_justificativa,
    oc.id_origem_ocorrencia
        """

_1746_queries = {
    "chamado": {
        "dataset_id": "adm_central_atendimento_1746",
        "partition_columns": "dt_inicio",
        "lower_bound_date": "2021-01-01",
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "dump_mode": "append",
        "execute_query": QUERY_CHAMADO_1746_DS,
    },
}
_1746_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 3, 21, 2, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
    db_database="REPLICA1746",
    db_host="10.70.1.34",
    db_port="1433",
    db_type="sql_server",
    dataset_id="adm_central_atendimento_1746",
    vault_secret_path="clustersql2",
    table_parameters=_1746_queries,
)

_1746_daily_update_schedule = Schedule(clocks=untuple(_1746_clocks))
