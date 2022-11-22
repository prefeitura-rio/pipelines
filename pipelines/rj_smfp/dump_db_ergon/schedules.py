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
        SELECT NUMFUNC,NUMVINC,DTINI,DTFIM,TIPOFREQ,CODFREQ,MOTIVO,PONTPUBL,DTPREVFIM,FLEX_CAMPO_01,FLEX_CAMPO_02,FLEX_CAMPO_03,FLEX_CAMPO_04,FLEX_CAMPO_05,PONTLEI,EMP_CODIGO,FLEX_CAMPO_06,FLEX_CAMPO_07,FLEX_CAMPO_08,FLEX_CAMPO_09,FLEX_CAMPO_10,ID_REG,REQLICAFAST,RESULTPRONT,FLEX_CAMPO_11,FLEX_CAMPO_12,FLEX_CAMPO_13,FLEX_CAMPO_14,FLEX_CAMPO_15,FLEX_CAMPO_16,FLEX_CAMPO_17,FLEX_CAMPO_18,FLEX_CAMPO_19,FLEX_CAMPO_20,FLEX_CAMPO_21,FLEX_CAMPO_22,FLEX_CAMPO_23,FLEX_CAMPO_24,FLEX_CAMPO_25,FLEX_CAMPO_26,FLEX_CAMPO_27,FLEX_CAMPO_28,FLEX_CAMPO_29,FLEX_CAMPO_30,FLEX_CAMPO_31,FLEX_CAMPO_32,FLEX_CAMPO_33,FLEX_CAMPO_34,FLEX_CAMPO_35,FLEX_CAMPO_36,FLEX_CAMPO_37,FLEX_CAMPO_38,FLEX_CAMPO_39,FLEX_CAMPO_40,FLEX_CAMPO_41,FLEX_CAMPO_42,FLEX_CAMPO_43,FLEX_CAMPO_44,FLEX_CAMPO_45,PONTPROC
        FROM ERGON.LIC_AFAST
        """,
    },
    "frequencias": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "DTINI",
        "dump_mode": "append",
        "execute_query": """
        SELECT NUMFUNC,NUMVINC,DTINI,DTFIM,TIPOFREQ,CODFREQ,QUANTIDADE,PONTPUBL,OBS,FLEX_CAMPO_01,FLEX_CAMPO_02,FLEX_CAMPO_03,FLEX_CAMPO_04,FLEX_CAMPO_05,PONTLEI,EMP_CODIGO,HORA_ENTRADA,HORA_SAIDA,ID_REG,FLEX_CAMPO_06,FLEX_CAMPO_07,FLEX_CAMPO_08,FLEX_CAMPO_09,FLEX_CAMPO_10,FLEX_CAMPO_11,FLEX_CAMPO_12,FLEX_CAMPO_13,FLEX_CAMPO_14,FLEX_CAMPO_15,FLEX_CAMPO_16,FLEX_CAMPO_17,FLEX_CAMPO_18,FLEX_CAMPO_19,FLEX_CAMPO_20,FLEX_CAMPO_21,FLEX_CAMPO_22,FLEX_CAMPO_23,FLEX_CAMPO_24,FLEX_CAMPO_25,FLEX_CAMPO_26,FLEX_CAMPO_27,FLEX_CAMPO_28,FLEX_CAMPO_29,FLEX_CAMPO_30,FLEX_CAMPO_31,FLEX_CAMPO_32,FLEX_CAMPO_33,FLEX_CAMPO_34,FLEX_CAMPO_35,FLEX_CAMPO_36,FLEX_CAMPO_37,FLEX_CAMPO_38,FLEX_CAMPO_39,FLEX_CAMPO_40,FLEX_CAMPO_41,FLEX_CAMPO_42,FLEX_CAMPO_43,FLEX_CAMPO_44,FLEX_CAMPO_45,FLEX_CAMPO_46,FLEX_CAMPO_47,FLEX_CAMPO_48,FLEX_CAMPO_49,FLEX_CAMPO_50,PONTPROC  
        FROM ERGON.FREQUENCIAS
        """,
    },
    "vantagens": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "partition_columns": "DTINI",
        "dump_mode": "append",
        "execute_query": """
        SELECT ,NUMFUNC,NUMVINC,VANTAGEM,DTINI,DTFIM,VALOR,INFO,TIPO_INCORPORACAO,PERC_INC_FUNCAO,INC_TABELAVENC,INC_REFERENCIA,PERC_INC_GRAT_FUNCAO,INC_GRAT_TABVENC,INC_GRAT_REFERENCIA,INCORP_EXTRA_TIPOFREQ_1,QUANT_EXTRA_1,INCORP_EXTRA_TIPOFREQ_2,QUANT_EXTRA_2,OBS,PONTPUBL,VALOR2,INFO2,VALOR3,INFO3,VALOR4,INFO4,VALOR5,INFO5,VALOR6,INFO6,PONTLEI,FLEX_CAMPO_01,FLEX_CAMPO_02,FLEX_CAMPO_03,FLEX_CAMPO_04,FLEX_CAMPO_05,FLEX_CAMPO_06,FLEX_CAMPO_07,FLEX_CAMPO_08,FLEX_CAMPO_09,FLEX_CAMPO_10,EMP_CODIGO,CHAVEVANT,ID_REG,FLEX_CAMPO_11,FLEX_CAMPO_12,FLEX_CAMPO_13,FLEX_CAMPO_14,FLEX_CAMPO_15,FLEX_CAMPO_16,FLEX_CAMPO_17,FLEX_CAMPO_18,FLEX_CAMPO_19,FLEX_CAMPO_20,ID_PROC_PES,FLEX_CAMPO_21,FLEX_CAMPO_22,FLEX_CAMPO_23,FLEX_CAMPO_24,FLEX_CAMPO_25,FLEX_CAMPO_26,FLEX_CAMPO_27,FLEX_CAMPO_28,FLEX_CAMPO_29,FLEX_CAMPO_30,PONTPROC
        FROM ERGON.VANTAGENS
        """,
    },
    "total_contagem": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT NUMFUNC,NUMVINC,DTINI,DTFIM,TIPOFREQ,CODFREQ,MOTIVO,PONTPUBL,DTPREVFIM,FLEX_CAMPO_01,FLEX_CAMPO_02,FLEX_CAMPO_03,FLEX_CAMPO_04,FLEX_CAMPO_05,PONTLEI,EMP_CODIGO,FLEX_CAMPO_06,FLEX_CAMPO_07,FLEX_CAMPO_08,FLEX_CAMPO_09,FLEX_CAMPO_10,ID_REG,REQLICAFAST,RESULTPRONT,FLEX_CAMPO_11,FLEX_CAMPO_12,FLEX_CAMPO_13,FLEX_CAMPO_14,FLEX_CAMPO_15,FLEX_CAMPO_16,FLEX_CAMPO_17,FLEX_CAMPO_18,FLEX_CAMPO_19,FLEX_CAMPO_20,FLEX_CAMPO_21,FLEX_CAMPO_22,FLEX_CAMPO_23,FLEX_CAMPO_24,FLEX_CAMPO_25,FLEX_CAMPO_26,FLEX_CAMPO_27,FLEX_CAMPO_28,FLEX_CAMPO_29,FLEX_CAMPO_30,FLEX_CAMPO_31,FLEX_CAMPO_32,FLEX_CAMPO_33,FLEX_CAMPO_34,FLEX_CAMPO_35,FLEX_CAMPO_36,FLEX_CAMPO_37,FLEX_CAMPO_38,FLEX_CAMPO_39,FLEX_CAMPO_40,FLEX_CAMPO_41,FLEX_CAMPO_42,FLEX_CAMPO_43,FLEX_CAMPO_44,FLEX_CAMPO_45,PONTPROC
        FROM ERGON.TOTAL_CONTA
        """,
    },
    "pre_contagem": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT FINALIDADE,NUMFUNC,NUMVINC,PERIODOS,OFFSET,DTINI,COMPLEMENTO,INFO1,INFO2,EMP_CODIGO,FLEX_CAMPO_01,FLEX_CAMPO_02,FLEX_CAMPO_03,FLEX_CAMPO_04,FLEX_CAMPO_05,FLEX_CAMPO_06,FLEX_CAMPO_07,FLEX_CAMPO_08,FLEX_CAMPO_09,FLEX_CAMPO_10,FLEX_CAMPO_11,FLEX_CAMPO_12,FLEX_CAMPO_13,FLEX_CAMPO_14,FLEX_CAMPO_15,FLEX_CAMPO_16,FLEX_CAMPO_17,FLEX_CAMPO_18,FLEX_CAMPO_19,FLEX_CAMPO_20,FLEX_DCAMPO_01,FLEX_DCAMPO_02,FLEX_DCAMPO_03,FLEX_DCAMPO_04,FLEX_DCAMPO_05,FLEX_DCAMPO_06,FLEX_DCAMPO_07,FLEX_DCAMPO_08,FLEX_DCAMPO_09,FLEX_DCAMPO_10,FLEX_NCAMPO_01,FLEX_NCAMPO_02,FLEX_NCAMPO_03,FLEX_NCAMPO_04,FLEX_NCAMPO_05,FLEX_NCAMPO_06,FLEX_NCAMPO_07,FLEX_NCAMPO_08,FLEX_NCAMPO_09,FLEX_NCAMPO_10,FLEX_NCAMPO_11,FLEX_NCAMPO_12,FLEX_NCAMPO_13,FLEX_NCAMPO_14,FLEX_NCAMPO_15,FLEX_NCAMPO_16,FLEX_NCAMPO_17,FLEX_NCAMPO_18,FLEX_NCAMPO_19,FLEX_NCAMPO_20,FLEX_CAMPO_21,FLEX_CAMPO_22,FLEX_CAMPO_23,FLEX_CAMPO_24,FLEX_CAMPO_25,CHAVE
        FROM ERGON.PRE_CONTA
        """,
    },
    "averbacoes": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT NUMFUNC,NUMVINC,CHAVE,DTINI,DTFIM,INSTITUICAO,TIPOTEMPO,DATA_A_CONTAR,TOTAL_DIAS,MOTIVO,DATA_REMOCAO,MOTIVO_REMOCAO,PONTPUBL,SOBREPOE,FLEX_CAMPO_01,FLEX_CAMPO_02,FLEX_CAMPO_03,FLEX_CAMPO_04,FLEX_CAMPO_05,EMP_CODIGO,OBS,FLEX_CAMPO_06,FLEX_CAMPO_07,FLEX_CAMPO_08,FLEX_CAMPO_09,FLEX_CAMPO_10,REGPREV,FLEX_NCAMPO_01,FLEX_NCAMPO_02,FLEX_NCAMPO_03,FLEX_NCAMPO_04,FLEX_NCAMPO_05,FLEX_NCAMPO_06,FLEX_NCAMPO_07,FLEX_NCAMPO_08,FLEX_NCAMPO_09,FLEX_NCAMPO_10,FLEX_CAMPO_11,FLEX_CAMPO_12,FLEX_CAMPO_13,FLEX_CAMPO_14,FLEX_CAMPO_15,FLEX_CAMPO_16,FLEX_CAMPO_17,FLEX_CAMPO_18,FLEX_CAMPO_19,FLEX_CAMPO_20,FLEX_CAMPO_21,FLEX_CAMPO_22,FLEX_CAMPO_23,FLEX_CAMPO_24,FLEX_CAMPO_25
        FROM ERGON.AVERBACOES_CONTA
        """,
    },
    "averbacoes_contagem": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT NUMFUNC,NUMVINC,CHAVEAVERB,FINALIDADE,DIAS,EVENTOS,INFO1,INFO2,COMPLEMENTO,EMP_CODIGO
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
        EMP_CODIGO, AFAST_COD, AFAST_DESCR, AFAST_INCID_FERIAS, AFAST_INCID_TRIENIO, AFAST_INCID_LIC_ESP, AFAST_INCID_TEMPO_SERV, AFAST_INCID_PGTO, AFAST_INCID_IDENT_PROC, AFAST_INCID_PREVIS, AFAST_INCID_IDENT_EXCL, AFAST_INCID_CCFG, AFAST_INCID_FREQ, AFAST_INCID_APOSENT, AFAST_INCID_MTS
        FROM SIMPA.SIRHU_DBTABELAS_AFASTAMENTO;
        """,
    },
    "tipo_tempo": {
        "materialize_after_dump": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
        SELECT SIGLA, NOME, APOSENTADORIA, FERIAS, DIAS_FER, ADICTSERV, LICESP, DIAS_LICESP, ADICTCHEFIA, PROGRESSAO, AVALIACAO, TIPO, TIPO_INFO_AVERB, PONTLEI, FLEX_CAMPO_01, FLEX_CAMPO_02, FLEX_CAMPO_03, FLEX_CAMPO_04, FLEX_CAMPO_05, FLEX_CAMPO_06, FLEX_CAMPO_07, FLEX_CAMPO_08, FLEX_CAMPO_09, FLEX_CAMPO_10
        FROM ERGON.TIPO_TEMPO
        """,
    },
    "ficha_financeira": {
        "materialize_after_dump": False,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "partition_columns": "MES_ANO_FOLHA",
        "execute_query": """
        SELECT MES_ANO_FOLHA,NUM_FOLHA,LANCAMENTO,NUMFUNC,NUMVINC,LINHA,NUMPENS,MES_ANO_DIREITO,RUBRICA,TIPO_RUBRICA,DESC_VANT,COMPLEMENTO,VALOR,CORRECAO,EXECUCAO,EMP_CODIGO
        FROM ERGON.FICHAS_FINANCEIRAS
        """,
    },
    "ficha_financeira_contabil": {
        "materialize_after_dump": False,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "partition_columns": "MES_ANO_FOLHA",
        "execute_query": """
        SELECT MES_ANO_FOLHA,NUM_FOLHA,NUMFUNC,NUMVINC,NUMPENS,SETOR,SECRETARIA,TIPO_FUNC,ATI_INAT_PENS,DETALHA,RUBRICA,TIPO_RUBRICA,MES_ANO_DIREITO,DESC_VANT,VALOR,COMPLEMENTO,TIPO_CLASSIF,CLASSIFICACAO,TIPO_CLASSIF_FR,CLASSIF_FR,ELEMDESP,TIPORUB,EMP_CODIGO
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
