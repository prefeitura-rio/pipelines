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
# SISCOB Schedules
#
#####################################

siscob_queries = {
    # "obra": {
    #     "materialize_after_dump": True,
    #     "materialization_mode": "prod",
    #     "dump_mode": "overwrite",
    #     "execute_query": """
    #         Select
    #             CD_OBRA,
    #             DS_TITULO,
    #             ORGAO_CONTRATANTE,
    #             ORGAO_EXECUTOR,
    #             NR_PROCESSO,
    #             OBJETO,
    #             NM_FAVORECIDO,
    #             CNPJ,
    #             NR_LICITACAO,
    #             MODALIDADE,
    #             DT_ASS_CONTRATO,
    #             DT_INICIO_OBRA,
    #             DT_TERMINO_PREVISTO,
    #             DT_TERMINO_ATUAL,
    #             NR_CONTRATO,
    #             AA_EXERCICIO,
    #             SITUACAO,
    #             VL_ORCADO_C_BDI,
    #             VL_CONTRATADO,
    #             VL_VIGENTE,
    #             PC_MEDIDO
    #         from dbo.fuSEGOVI_Dados_da_Obra();""",
    # },
    "medicao": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            Select
                CD_OBRA,
                NR_MEDICAO,
                CD_ETAPA,
                TP_MEDICAO_D,
                DT_INI_MEDICAO,
                DT_FIM_MEDICAO,
                VL_FINAL
            from dbo.fuSEGOVI_Medicoes();
            """,
    },
    "termo_aditivo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            Select
                CD_OBRA,
                NR_DO_TERMO,
                TP_ACERTO,
                DT_DO,
                DT_AUTORIZACAO,
                VL_ACERTO
            from dbo.fuSEGOVI_Termos_Aditivos();
            """,
    },
    "cronograma_financeiro": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            Select
                CD_OBRA,
                ETAPA,
                DT_INICIO_ETAPA,
                DT_FIM_ETAPA,
                PC_PERCENTUAL,
                VL_ESTIMADO
            from dbo.fuSEGOVI_Cronograma_Financeiro();
            """,
    },
    "localizacao": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            Select
                CD_OBRA,
                ENDERECO,
                NM_BAIRRO,
                NM_RA,
                NM_AP
            from dbo.fuSEGOVI_Localizacoes_obra();
            """,
    },
    "cronograma_alteracao": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            Select
                CD_OBRA,
                NR_PROCESSO,
                TP_ALTERACAO,
                DT_PUBL_DO,
                CD_ETAPA,
                NR_PRAZO,
                DT_VALIDADE,
                DS_OBSERVACAO
            from dbo.fuSEGOVI_Alteração_de_Cronograma();
            """,
    },
}

siscob_clocks = generate_dump_db_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2022, 10, 2, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMI_AGENT_LABEL.value,
    ],
    db_database="SISCOB200",
    db_host="10.70.1.34",
    db_port="1433",
    db_type="sql_server",
    dataset_id="infraestrutura_siscob_obras",
    vault_secret_path="siscob",
    table_parameters=siscob_queries,
)

siscob_update_schedule = Schedule(clocks=untuple(siscob_clocks))
