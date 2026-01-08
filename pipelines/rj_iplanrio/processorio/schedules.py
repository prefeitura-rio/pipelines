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
# Processorio Schedules
#
#####################################

_processorio_infra_query = {
    "classificacao": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                ID_CLASSIFICACAO,
                DESCR_CLASSIFICACAO,
                HIS_ATIVO,
                CODIFICACAO
            FROM SIGA.VW_CLASSIFICACAO
        """,  # noqa
    },
    "documento_tempo": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                SIGLA_DOC,
                DT_PRIMEIRAASSINATURA,
                ULTIMO_ID_MOV,
                DT_FINALIZACAO,
                ARQUIVADO,
                ID_MOBIL,
                TEMPO_TRAMITACAO,
                ID_LOTA_RESP,
                LOTACAO_RESP,
                DATA_COM_RESP_ATUAL
            FROM SIGA.DOCUMENTOS_TEMPO
        """,  # noqa
    },
    "forma_documento": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                ID_FORMA_DOC,
                DESCR_FORMA_DOC,
                SIGLA_FORMA_DOC,
                ID_TIPO_FORMA_DOC
            FROM SIGA.VW_FORMA_DOCUMENTO
        """,  # noqa
    },
    "lotacao": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                ID_LOTACAO,
                DATA_INI_LOT,
                DATA_FIM_LOT,
                NOME_LOTACAO,
                ID_LOTACAO_PAI,
                SIGLA_LOTACAO,
                ID_ORGAO_USU,
                IS_EXTERNA_LOTACAO
            FROM CORPORATIVO.VW_LOTACAO
        """,  # noqa
    },
    "mobil": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                ID_MOBIL,
                ID_DOC
            FROM SIGA.VW_MOBIL
        """,  # noqa
    },
    "mobil_tipo": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                *
            FROM SIGA.VW_TIPO_MOBIL
        """,  # noqa
    },
    "modelo": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                ID_MOD,
                NM_MOD,
                DESC_MOD,
                HIS_ID_INI,
                HIS_IDC_INI,
                HIS_IDC_FIM,
                HIS_ATIVO,
                IS_PETICIONAMENTO
            FROM SIGA.VW_MODELO
        """,  # noqa
    },
    "movimentacao": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "append",
        "partition_columns": "DT_MOV",
        "partition_date_format": "%Y-%m-%d",
        "lower_bound_date": "current_month",
        "execute_query": """
            SELECT
                ID_MOV,
                ID_TP_MOV,
                ID_CADASTRANTE,
                ID_LOTA_CADASTRANTE,
                CAST(CAST(DT_MOV AS VARCHAR(23)) AS DATE) DT_MOV,
                CAST(CAST(DT_FIM_MOV AS VARCHAR(23)) AS DATE) DT_FIM_MOV,
                ID_MOV_REF,
                ID_MOBIL
            FROM SIGA.VW_MOVIMENTACAO
        """,  # noqa
    },
    "movimentacao_tipo": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                *
            FROM SIGA.VW_TIPO_MOVIMENTACAO
        """,  # noqa
    },
    "nivel_acesso": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                ID_NIVEL_ACESSO,
                NM_NIVEL_ACESSO
            FROM SIGA.VW_NIVEL_ACESSO
        """,  # noqa
    },
    "orgao_usuario": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                ID_ORGAO_USU,
                NM_ORGAO_USU,
                SIGLA_ORGAO_USU,
                COD_ORGAO_USU,
                IS_EXTERNO_ORGAO_USU,
                HIS_ATIVO,
                IS_PETICIONAMENTO
            FROM CORPORATIVO.VW_ORGAO_USUARIO
        """,  # noqa
    },
    "documento": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "append",
        "partition_columns": "DT_DOC",
        "partition_date_format": "%Y-%m-%d",
        "lower_bound_date": "current_month",
        "execute_query": """
            SELECT
                *
            FROM SIGA.VW_DOCUMENTO
        """,  # noqa
        "dbt_alias": True,
    },
    "documentos_hora": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "interval": timedelta(hours=1),
        "execute_query": """
            SELECT
            HORA,
            QTD_HORA
            FROM SIGA.VW_MOVIMENTOS_POR_HORA
        """,  # noqa
        "dbt_alias": False,
    },
    "ex_forma_documento": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                ID_FORMA_DOC,
                DESCR_FORMA_DOC,
                SIGLA_FORMA_DOC,
                ID_TIPO_FORMA_DOC
            FROM SIGA.VW_FORMA_DOCUMENTO
        """,
    },
    "dp_pessoa": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "partition_columns": "DATA_INI_PESSOA",
        "partition_date_format": "%Y-%m-%d",
        "lower_bound_date": "current_month",
        "dump_mode": "append",
        "execute_query": """
            SELECT
                ID_PESSOA,
                DATA_INI_PESSOA,
                DATA_FIM_PESSOA,
                CPF_PESSOA,
                NOME_PESSOA,
                DATA_NASC_PESSOA,
                MATRICULA,
                ID_LOTACAO,
                ID_CARGO,
                ID_FUNCAO_CONFIANCA,
                SESB_PESSOA,
                EMAIL_PESSOA,
                TP_SERVIDOR_PESSOA,
                SIGLA_PESSOA,
                SEXO_PESSOA,
                GRAU_INSTRUCAO_PESSOA,
                TP_SANGUINEO_PESSOA,
                NACIONALIDADE_PESSOA,
                DATA_POSSE_PESSOA,
                DATA_NOMEACAO_PESSOA,
                DATA_PUBLICACAO_PESSOA,
                DATA_INICIO_EXERCICIO_PESSOA,
                ATO_NOMEACAO_PESSOA,
                SITUACAO_FUNCIONAL_PESSOA,
                ID_PROVIMENTO,
                NATURALIDADE_PESSOA,
                FG_IMPRIME_END,
                DSC_PADRAO_REFERENCIA_PESSOA,
                ID_ORGAO_USU,
                IDE_PESSOA,
                ID_PESSOA_INICIAL,
                ENDERECO_PESSOA,
                BAIRRO_PESSOA,
                CIDADE_PESSOA,
                CEP_PESSOA,
                TELEFONE_PESSOA,
                RG_PESSOA,
                RG_ORGAO_PESSOA,
                RG_DATA_EXPEDICAO_PESSOA,
                RG_UF_PESSOA,
                ID_ESTADO_CIVIL,
                ID_TP_PESSOA,
                NOME_EXIBICAO,
                HIS_IDC_INI,
                HIS_IDC_FIM,
                NOME_PESSOA_AI
            FROM CORPORATIVO.VW_DP_PESSOA
        """,
    },
    "cp_identidade": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT
            ID_IDENTIDADE,
            ID_TP_IDENTIDADE,
            ID_PESSOA,
            DATA_CRIACAO_IDENTIDADE,
            DATA_EXPIRACAO_IDENTIDADE,
            DATA_CANCELAMENTO_IDENTIDADE,
            ID_ORGAO_USU,
            LOGIN_IDENTIDADE,
            SENHA_IDENTIDADE,
            SENHA_IDENTIDADE_CRIPTO,
            SENHA_IDENTIDADE_CRIPTO_SINC,
            HIS_ID_INI,
            HIS_DT_INI,
            HIS_IDC_INI,
            HIS_DT_FIM,
            HIS_IDC_FIM,
            HIS_ATIVO,
            PIN_IDENTIDADE
        FROM CORPORATIVO.CP_IDENTIDADE
    """,
    },
    "ex_tipo_forma_documento": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT
            ID_TIPO_FORMA_DOC,
            DESC_TIPO_FORMA_DOC,
            NUMERACAO_UNICA
        FROM SIGA.EX_TIPO_FORMA_DOCUMENTO
""",
    },
}

processorio_infra_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 3, 21, 2, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
    db_database="SIGADOC.PCRJ",
    db_host="10.70.6.64",
    db_port="1521",
    db_type="oracle",
    dataset_id="adm_processo_interno_processorio",
    vault_secret_path="processorio-prod",
    table_parameters=_processorio_infra_query,
)

processorio_infra_daily_update_schedule = Schedule(
    clocks=untuple(processorio_infra_clocks)
)
