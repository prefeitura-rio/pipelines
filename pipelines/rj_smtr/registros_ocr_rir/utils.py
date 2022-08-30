# -*- coding: utf-8 -*-
"""
General purpose functions for the monitoramento_rock_in_rio project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto monitoramento_rock_in_rio.
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
# from pipelines.rj_smtr.monitoramento_rock_in_rio.utils import foo
# foo()
# ```
#
###############################################################################
from pipelines.rj_smtr.utils import log_critical


def log_error(error):
    """
    logs error to Discord
    """
    message = f"""
        @here
        Rock In Rio 2022
        Captura falhou com erro:
        {error}
        """
    log_critical(message)
