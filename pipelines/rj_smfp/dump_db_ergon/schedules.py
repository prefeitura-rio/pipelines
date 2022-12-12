# -*- coding: utf-8 -*-
# flake8: noqa: E501
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
# Ergon Schedules
#
#####################################

ergon_queries = {
    "cargo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_CARGOS_",
    },
    "categoria": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_CATEGORIAS_",
    },
    "empresa": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_EMPRESAS",
    },
    "matricula": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ERG_MATRICULAS",
    },
    "fita_banco": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "MES_ANO",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FITA_BANCO",
    },
    "folha_empresa": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "MES_ANO",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FOLHAS_EMP",
    },
    "forma_provimento": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FORMAS_PROV_",
    },
    "funcionario": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_FUNCIONARIOS",
    },
    "horario_trabalho": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_HORARIO_TRAB_",
    },
    "setor": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_HSETOR_",
    },
    "jornada": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_JORNADAS_",
    },
    "orgaos_externos": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ORGAOS_EXTERNOS",
    },
    "orgaos_regime_juridico": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_ORGAOS_REGIMES_JUR_",
    },
    "provimento": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_PROVIMENTOS_EV",
    },
    "regime_juridico": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_REGIMES_JUR_",
    },
    "tipo_folha": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_FOLHA",
    },
    "tipo_orgao": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_ORGAO",
    },
    "tipo_vinculo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_TIPO_VINC_",
    },
    "vinculo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM C_ERGON.VW_DLK_ERG_VINCULOS",
    },
    "licenca_afastamento": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "DTINI",
        "dump_mode": "append",
        "execute_query": """
        SELECT NUMFUNC,NUMVINC,DTINI,DTFIM,TIPOFREQ,CODFREQ,MOTIVO,DTPREVFIM,FLEX_CAMPO_01,
            FLEX_CAMPO_02,EMP_CODIGO,FLEX_CAMPO_07
        FROM ERGON.LIC_AFAST
        """,
    },
    "frequencia": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "DTINI",
        "dump_mode": "append",
        "execute_query": """
        SELECT NUMFUNC,NUMVINC,DTINI,DTFIM,TIPOFREQ,CODFREQ,OBS,EMP_CODIGO
        FROM ERGON.FREQUENCIAS
        """,
    },
    "vantagens": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "DTINI",
        "dump_mode": "append",
        "execute_query": """
        SELECT NUMFUNC,NUMVINC,VANTAGEM,DTINI,DTFIM,VALOR,INFO,TIPO_INCORPORACAO,PERC_INC_FUNCAO,
            INC_TABELAVENC,INC_REFERENCIA,OBS,VALOR2,INFO2,VALOR3,INFO3,VALOR4,INFO4,VALOR5,INFO5,
            VALOR6,INFO6,FLEX_CAMPO_05,EMP_CODIGO,CHAVEVANT
        FROM ERGON.VANTAGENS
        """,
    },
    "total_contagem": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT CHAVE, NUMFUNC,NUMVINC,FINALIDADE,DIASTOT,DIASFPUB,DIASFPUBESP,TOTAL_PERIODOS,
            TOTAL_ANOS,DATA_PROXIMO,NOME_PROXIMO,EMP_CODIGO
        FROM ERGON.TOTAL_CONTA
        """,
    },
    "pre_contagem": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT FINALIDADE,NUMFUNC,NUMVINC,PERIODOS,OFFSET,DTINI,EMP_CODIGO,FLEX_CAMPO_01
        FROM ERGON.PRE_CONTA
        """,
    },
    "averbacoes": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT NUMFUNC,NUMVINC,CHAVE,DTINI,DTFIM,INSTITUICAO,TIPOTEMPO,DATA_A_CONTAR,TOTAL_DIAS,
            MOTIVO,SOBREPOE,EMP_CODIGO,OBS,REGPREV
        FROM ERGON.AVERBACOES_CONTA
        """,
    },
    "averbacoes_contagem": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT NUMFUNC,NUMVINC,CHAVEAVERB,FINALIDADE,DIAS,EMP_CODIGO
        FROM ERGON.AVERB_OQUE_CONTA
        """,
    },
    "frequencia_antigo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT M9, SF_OCORRENCIA, SF_DT_OC_Y2
        FROM C_ERGON.VW_SIMPA_SIRHU_FREQUENCIA_GBP;
        """,
    },
    "afastamento_antigo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT M10, SA_DT_AFAS_Y2, SA_DT_PRER_Y2, SA_DT_RETR_Y2
        FROM C_ERGON.VW_SIMPA_SIRHU_AFASTAMENTO_GBP
        """,
    },
    "afastamento_antigo_nomes": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT
        EMP_CODIGO, AFAST_COD, AFAST_DESCR
        FROM SIMPA.SIRHU_DBTABELAS_AFASTAMENTO;
        """,
    },
    "tipo_tempo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT SIGLA, NOME, APOSENTADORIA, FERIAS, DIAS_FER, ADICTSERV, LICESP, DIAS_LICESP,
            ADICTCHEFIA, PROGRESSAO
        FROM ERGON.TIPO_TEMPO
        """,
    },
    "ficha_financeira": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "partition_columns": "MES_ANO_FOLHA",
        "execute_query": """
        SELECT MES_ANO_FOLHA,NUM_FOLHA,LANCAMENTO,NUMFUNC,NUMVINC,LINHA,NUMPENS,MES_ANO_DIREITO,
            RUBRICA,TIPO_RUBRICA,DESC_VANT,COMPLEMENTO,VALOR,CORRECAO,EXECUCAO,EMP_CODIGO
        FROM ERGON.FICHAS_FINANCEIRAS
        """,
    },
    "ficha_financeira_contabil": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "partition_columns": "MES_ANO_FOLHA",
        "execute_query": """
        SELECT MES_ANO_FOLHA,NUM_FOLHA,NUMFUNC,NUMVINC,NUMPENS,SETOR,SECRETARIA,TIPO_FUNC,
            ATI_INAT_PENS,DETALHA,RUBRICA,TIPO_RUBRICA,MES_ANO_DIREITO,DESC_VANT,VALOR,COMPLEMENTO,
            TIPO_CLASSIF,CLASSIFICACAO,TIPO_CLASSIF_FR,CLASSIF_FR,ELEMDESP,TIPORUB,EMP_CODIGO
        FROM ERGON.IPL_PT_FICHAS
        """,
    },
}

ergon_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 11, 9, 10, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    db_database="P01.PCRJ",
    db_host="10.70.6.21",
    db_port="1526",
    db_type="oracle",
    dataset_id="recursos_humanos_ergon",
    vault_secret_path="ergon-prod",
    table_parameters=ergon_queries,
)

ergon_monthly_update_schedule = Schedule(clocks=untuple(ergon_clocks))
