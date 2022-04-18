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


def get_clock(dataset_id, table_id, count):
    return IntervalClock(
        interval=timedelta(days=30),
        start_date=datetime(
            2022, 1, 1, 8, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=3 * count),
        labels=[
            constants.EMD_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "mode": "prod",
        },
    )


#####################################
#
# Ergon Schedules
#
#####################################

ergon_views = {
    "VW_DLK_ERG_CARGOS_": "cargo",
    "VW_DLK_ERG_CATEGORIAS_": "categoria",
    "VW_DLK_ERG_EMPRESAS": "empresa",
    "VW_DLK_ERG_ERG_MATRICULAS": "matricula",
    "VW_DLK_ERG_FITA_BANCO": "fita_banco",
    "VW_DLK_ERG_FOLHAS_EMP": "folha_empresa",
    "VW_DLK_ERG_FORMAS_PROV_": "forma_prov",
    "VW_DLK_ERG_FUNCIONARIOS": "funcionario",
    "VW_DLK_ERG_HORARIO_TRAB_": "horario_trabalho",
    "VW_DLK_ERG_HSETOR_": "h_setor",
    "VW_DLK_ERG_JORNADAS_": "jornada",
    "VW_DLK_ERG_ORGAOS_EXTERNOS": "orgaos_externos",
    "VW_DLK_ERG_ORGAOS_REGIMES_JUR_": "orgaos_regime_juridico",
    "VW_DLK_ERG_PROVIMENTOS_EV": "provimentos_ev",
    "VW_DLK_ERG_REGIMES_JUR_": "regime_juridico",
    "VW_DLK_ERG_TIPO_FOLHA": "tipo_folha",
    "VW_DLK_ERG_TIPO_ORGAO": "tipo_orgao",
    "VW_DLK_ERG_TIPO_VINC_": "tipo_vinculo",
    "VW_DLK_ERG_VINCULOS": "vinculo",
}


ergon_clocks = [
    get_clock("administracao_recursos_humanos_folha_salarial", table_id, count)
    for count, (_, table_id) in enumerate(ergon_views.items())
]

ergon_monthly_update_schedule = Schedule(clocks=untuple(ergon_clocks))

#####################################
#
# SME Schedules
#
#####################################

sme_views = {
    "dependencia": "dependencia",
    "escola": "escola",
    "frequencia": "frequencia",
    "turma": "turma",
}


sme_clocks = [
    get_clock("educacao_basica", table_id, count)
    for count, (_, table_id) in enumerate(sme_views.items())
]

sme_monthly_update_schedule = Schedule(clocks=untuple(sme_clocks))


#####################################
#
# 1746 Schedules
#
#####################################

_1746_views = {
    "chamado": "chamado",
}


_1746_clocks = [
    get_clock("administracao_servicos_publicos_1746", table_id, count)
    for count, (_, table_id) in enumerate(_1746_views.items())
]

_1746_monthly_update_schedule = Schedule(clocks=untuple(_1746_clocks))
