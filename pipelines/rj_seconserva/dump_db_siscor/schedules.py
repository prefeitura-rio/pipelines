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
# SISCOR Schedules
#
#####################################

siscor_queries = {
    "processo_para_autorizacao_de_obra": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
              distinct STR(po.nmr_processo, 6, 0) + '/' + po.ano_processo as Processo,
              po.sqnc_processo,
              case po.sqncl_tp_processo when 0 then tpr.tp_processo
                  else STR(po.sqncl_tp_processo,1)
                  + '° ' + tpr.tp_processo
                  END AS dscr_tp_processo,
              po.dt_protocolo, po.dt_inicio_obra, po.dt_fim_obra,
              po.area_ocupacao, po.observacao, po.prz_execucao, po.dt_aprovacao,
              nat.natureza_obra, tpo.tp_obra, lco.lcl_obra, vpfj.nome as empresa,
              lo.cdg_logradouro, lo.nm_lgrdr,
              po.dt_plenario, tps.dscr_situacao, tpp.dscr_parecer, po.nmr_licenca
              FROM PROCESSO_OCOR po
              INNER JOIN     NATUREZA_OBRA     nat ON
                  po.cdg_natureza = nat.cdg_natureza
              INNER JOIN     TIPO_OBRA tpo         ON
                  po.cdg_tp_obra = tpo.cdg_tp_obra
              INNER JOIN     TIPO_PROCESSO tpr    ON
                  po.cdg_tp_processo = tpr.cdg_tp_processo
              INNER JOIN     TIPO_PARECER tpp    ON
                  po.cdg_parecer = tpp.cdg_parecer
              INNER JOIN     TIPO_SITUACAO tps    ON
                  po.cdg_situacao = tps.cdg_situacao
              INNER JOIN     LOCAL_OBRA lco        ON
                  po.cdg_lcl_obra = lco.cdg_lcl_obra
              INNER join V_LOCALIZACAO lo
              on lo.nmr_processo = po.nmr_processo
              and lo.ano_processo = po.ano_processo
              left join requerente rp
              on rp.nmr_processo = po.nmr_processo
              and rp.ano_processo = po.ano_processo
              left join v_ps_FscJrdc vpfj
              on vpfj.sqnc_pessoa = rp.sqnc_pessoa_requerente
            """,
    },
}

siscor_clocks = generate_dump_db_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2022, 12, 19, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SECONSERVA_AGENT_LABEL.value,
    ],
    db_database="siscor_seconserva",
    db_host="10.70.11.61",
    db_port="1433",
    db_type="sql_server",
    dataset_id="infraestrutura_siscor_obras",
    vault_secret_path="db_siscor",
    table_parameters=siscor_queries,
)

siscor_update_schedule = Schedule(clocks=untuple(siscor_clocks))
