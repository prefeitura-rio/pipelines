# -*- coding: utf-8 -*-
"""
General purpose functions for the projeto_subsidio_sppo project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto projeto_subsidio_sppo.
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
# from pipelines.rj_smtr.projeto_subsidio_sppo.utils import foo
# foo()
# ```
#
###############################################################################
import pandas as pd


def melt_by_direction(df, id_vars, value_name="trip_id", var_name="aux"):
    return (
        df[id_vars + [f"{value_name}_ida", f"{value_name}_volta"]]
        .melt(id_vars, var_name=var_name, value_name=value_name)
        .dropna(subset=[value_name])
        .replace({f"{value_name}_ida": "ida", f"{value_name}_volta": "volta"})
    )
