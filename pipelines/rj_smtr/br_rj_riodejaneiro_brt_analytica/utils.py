# -*- coding: utf-8 -*-
"""
General purpose functions for the br_rj_riodejaneiro_brt_analytica project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_rj_riodejaneiro_brt_analytica.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar funções, basta fazer em código Python comum, como abaixo:
#
# ```
# def foo():
#     """
#     Function foo
#     """
#     print("foo")
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.rj_smtr.br_rj_riodejaneiro_brt_analytica.utils import foo
# foo()
# ```
#
###############################################################################

import pandas as pd
import os
from pipelines.rj_smtr.br_rj_riodejaneiro_brt_analytica.constants import constants


def get_intervalos():
    """Obtem os dados de intervalo de tempo para cada trecho historico percorrido"""

    path = os.path.join(constants.CURRENT_DIR, "Dados/intervalo_trechos.parquet")
    df_intervalos = pd.read_parquet(path, engine="fastparquet")

    df_intervalos["tempo_saida"] = pd.to_datetime(df_intervalos["tempo_saida"])
    df_intervalos["tempo_chegada"] = pd.to_datetime(df_intervalos["tempo_chegada"])
    df_intervalos["delta_tempo"] = pd.to_timedelta(df_intervalos["delta_tempo"])

    df_intervalos["hora"] = df_intervalos["tempo_saida"].dt.hour
    df_intervalos["dia_da_semana"] = df_intervalos.tempo_saida.dt.dayofweek
    df_intervalos["data"] = df_intervalos.tempo_saida.dt.date
    df_intervalos["horario"] = df_intervalos.tempo_saida.dt.time

    return df_intervalos


def get_day_hour_index():
    """Retorna um multindex com cada dia da semana e hora possivel"""

    return pd.MultiIndex.from_product(
        [range(0, 7), range(0, 24)], names=["dia_da_semana", "hora"]
    )


def fill_hours(df, cols, day_hour_index):
    """Preenche os dia-horas inexistentes no dataframe com o valor médio"""

    fill_value = df["delta_tempo"].mean()
    return (
        df.set_index(cols)
        .reindex(day_hour_index)[["delta_tempo"]]
        .fillna(fill_value)
        .reset_index()
    )
