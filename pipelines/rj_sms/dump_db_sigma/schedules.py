# -*- coding: utf-8 -*-
"""
Schedules for the SMS SIGMA dump_db pipeline.
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz

from pipelines.constants import constants
from pipelines.utils.dump_db.utils import generate_dump_db_schedules
from pipelines.utils.utils import untuple_clocks as untuple


#####################################
#
# SMS SIGMA Schedules
#
#####################################

_sigma_queries = {
    "classe": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                CD_GRUPO, 
                CD_CLASSE, 
                DS_CLASSE, 
                ST_STATUS
            FROM SIGMA.VW_CLASSE
        """,  # noqa
    },
    "fornecedor": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                CPF_CNPJ,
                TIPO_CPF_CNPJ,
                INSCRICAO_MUNICIPAL,
                INSCRICAO_ESTADUAL,
                RAZAO_SOCIAL,
                NOME_FANTASIA,
                NOME_CONTATO,
                EMAIL,
                EMAIL_CONTATO,
                FAX,
                DDD,
                DDI,
                RAMAL,
                TELEFONE,
                LOGRADOURO,
                NUMERO_PORTA,
                COMPLEMENTO,
                BAIRRO,
                MUNICIPIO,
                UF,
                CEP,
                ATIVO_INATIVO_BLOQUEADO,
                CD_NATUREZA_JURIDICA,
                DS_NATUREZA_JURIDICA,
                RAMO_ATIVIDADE,
                CD_PORTE_EMPRESA,
                DATA_ULTIMA_ATUALIZACAO,
                FORNECEDOR_EVENTUAL
            FROM SIGMA.VW_FORNECEDOR
        """,  # noqa
    },
    "fornecedor_sem_vinculo": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                CPF_CNPJ,
                TIPO_CPF_CNPJ,
                NOME,
                NUMERO_PORTA,
                COMPLEMENTO
            FROM SIGMA.VW_FORNECEDOR_SEM_VINCULO
        """,  # noqa
    },
    "grupo": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                CD_GRUPO, 
                DS_GRUPO, 
                ST_STATUS
            FROM SIGMA.VW_GRUPO
        """,  # noqa
    },
    "material": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                CD_MATERIAL,
                CD_GRUPO,
                CD_CLASSE,
                CD_SUBCLASSE,
                SEQUENCIAL,
                DV1,
                DV2,
                NM_PADRONIZADO,
                NM_COMPLEMENTAR_MATERIAL,
                UNIDADE,
                DS_DETALHE_MATERIAL,
                DT_DESATIVACAO,
                ST_STATUS,
                REMUME
            FROM SIGMA.VW_MATERIAL
        """,  # noqa
    },
    "movimentacao": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                CD_MATERIAL,
                CNPJ_FORNECEDOR,
                NOTA_FISCAL,
                SERIE,
                DATA_NOTA_FISCAL,
                QUANTIDADE_ITEM,
                PRECO_ITEM,
                TOTAL_ITEM,
                DATA_ULTIMA_ATUALIZACAO,
                CD_MOVIMENTACAO,
                DS_MOVIMENTACAO,
                TP_ALMOXARIFADO,
                CD_SECRETARIA,
                DS_SECRETARIA,
                CD_ALMOXARIFADO_DESTINO,
                DS_ALMOXARIFADO_DESTINO,
                CD_ALMOXARIFADO_ORIGEM,
                DS_ALMOXARIFADO_ORIGEM,
                CD_OS,
                DT_INI_CONTRATO_OS,
                DT_FIM_CONTRATO_OS,
                NR_EMPENHO,
                CNPJ_FABRICANTE
            FROM SIGMA.VW_MOVIMENTACAO
        """,  # noqa
    },
    "ramo_atividade": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                CD_RAMO, 
                DS_RAMO, 
                ST_RAMO
            FROM SIGMA.VW_RAMO_ATIVIDADE
        """,  # noqa
    },
    "servico": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                CD_SERV,
                CD_SEQ,
                CD_SERVICO,
                DS_SERVICO,
                ST_STATUS
            FROM SIGMA.VW_SERVICO
        """,  # noqa
    },
    "subclasse": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                CD_GRUPO,
                CD_CLASSE,
                CD_SUBCLASSE,
                DS_SUBCLASSE,
                ST_STATUS
            FROM SIGMA.VW_SUBCLASSE
        """,  # noqa
    },
    "unidade": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT 
                UNIDADE, 
                DS_UNIDADE
            FROM SIGMA.VW_UNIDADE
        """,  # noqa
    },
}

sigma_infra_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 3, 21, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    db_database="CP01.SMF",
    db_host="10.90.31.22",
    db_port="1521",
    db_type="oracle",
    dataset_id="saude_estoque_medicamentos_sigma",
    vault_secret_path="db-sigma",
    table_parameters=_sigma_queries,
)

sigma_daily_update_schedule = Schedule(clocks=untuple(sigma_infra_clocks))
