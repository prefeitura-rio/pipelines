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
                REMUME,
                ACONDICIONAMENTO,
                TP_GENERO,
                CD_MATERIAL_SUBSTITUTO,
                NM_FANTASIA,
                CD_COMPRASNET,
                TP_MATERIAL,
                ST_REFERENCIA,
                ST_SISTEMA_REGISTRO_PRECO,
                TERMOLABEL,
                ST_CONTROLADO,
                ST_PADRONIZADO,
                ST_USO_GERAL,
                ST_CONTINUADO,
                ST_TABELADO,
                ST_ITEM_SUSTENTAVEL,
                OBSERVACAO
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
        "interval": timedelta(days=7),
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
                DV,
                CD_SERVICO,
                DS_SERVICO,
                ST_STATUS,
                CD_COMPRASNET,
                NM_PADRONIZADO,
                UNIDADE_SERVICO,
                ST_RESPONSAVEL_TECNICO,
                ST_SISTEMA_REGISTRO_PRECO,
                CD_ATIVIDADE_ECONOMICA,
                CD_GRUPO_CAE,
                CD_SUBGRUPO_CAE,
                CD_ATIVIDADE_CAE,
                DS_ATIVIDADE_ECONOMICA,
                DS_SUBGRUPO_CAE,
                ST_TABELADO,
                ST_CADASTRO_FORNECEDOR
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
    "material_referencia": {
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
                CD_REFERENCIA,
                DS_REFERENCIA,
                ST_STATUS
            FROM SIGMA.VW_MATERIAL_REFERENCIA
        """,  # noqa
    },
    "usuario_sistema": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                NM_PRESTADOR_SERVICO,
                CPF,
                MATRICULA,
                NM_FUNCIONARIO,
                EMAIL_INSTITUCIONAL,
                EMAIL_ALTERNATIVO,
                TEL_CORPORATIVO1,
                TEL_CORPORATIVO2,
                TEL_ALTERNATIVO1,
                TEL_ALTERNATIVO2,
                CD_ORGAO_DESIGNACAO,
                DS_ORGAO_DESIGNACAO,
                CD_PERFIL,
                DS_PERFIL,
                PRIVILEGIO_ALMOXARIFADO,
                HORA_ACESSO_INI,
                MINUTO_ACESSO_INI,
                HORA_ACESSO_FIM,
                MINUTO_ACESSO_FIM,
                DT_INCLUSAO,
                ST_STATUS,
                ST_SITUACAO,
                CD_TERMINAL,
                DT_ULTIMA_SESSAO,
                HORA_ULTIMA_SESSAO
            FROM SIGMA.VW_USUARIO_SISTEMA
        """,  # noqa
    },
    "orgao": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                CD_ORGAO,
                TP_ORGAO,
                DS_TIPO_ORGAO,
                CD_ORGAO_PAI,
                CD_SECRETARIA_SDI,
                DESCRICAO,
                ENDERECO,
                COMPLEMENTO,
                CEP,
                NUMERO_PORTA,
                FAX1,
                FAX2,
                TEL1,
                TEL2,
                SIGLA,
                EMAIL,
                TP_UNIDADE,
                ST_STATUS,
                CNES,
                MATRICULA_RESPONSAVEL,
                NM_RESPONSAVEL,
                DT_RESPONSAVEL
            FROM SIGMA.VW_ORGAO
        """,  # noqa
    },
    "usuario_responsavel_auxiliar": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                CD_ORGAO,
                DS_ORGAO,
                MATRICULA,
                NM_FUNCIONARIO,
                TP_FUNCIONARIO,
                DT_INICIO,
                DT_TERMINO,
                DT_TERMO_RESPONSABILIDADE,
                DT_EXCEPCIONALIDADE,
                DT_PUBLICACAO_DESIGNACAO,
                ESCOLARIDADE,
                ST_CURSO_GESTAO_MATERIAL
            FROM SIGMA.VW_USUARIO_RESPONSAVEL_AUXILIAR
        """,  # noqa
    },
    "unidade_armazenadora": {
        "biglake_table": True,
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                CD_UNIDADE_ARMAZENADORA,
                DS_UNIDADE_ARMAZENADORA,
                TP_ALMOXARIFADO,
                CD_UNIDADE_ADMINISTRATIVA,
                DS_UNIDADE_ADMINISTRATIVA,
                ST_STATUS,
                CD_PROGRAMA_TRABALHO,
                CNES,
                ST_EXPR_MONETARIA_OITO_CASAS,
                TP_UNIDADE_ARMAZENADORA,
                MATRICULA_RESPONSAVEL,
                NM_RESPONSAVEL,
                DT_RESPONSAVEL,
                MATRICULA_SUBSTITUTO1,
                NM_SUBSTITUTO1,
                DT_SUBSTITUTO1,
                MATRICULA_SUBSTITUTO2,
                NM_SUBSTITUTO2,
                DT_SUBSTITUTO2
            FROM SIGMA.VW_UNIDADE_ARMAZENADORA
        """,  # noqa
    },
}

sigma_infra_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 3, 21, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    db_database="CP01.SMF",
    db_host="10.90.31.22",
    db_port="1521",
    db_type="oracle",
    dataset_id="compras_materiais_servicos_sigma",
    vault_secret_path="db-sigma",
    table_parameters=_sigma_queries,
)

compras_sigma_daily_update_schedule = Schedule(clocks=untuple(sigma_infra_clocks))
