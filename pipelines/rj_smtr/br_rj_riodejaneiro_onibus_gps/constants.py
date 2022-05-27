# -*- coding: utf-8 -*-
"""
Constant values for the rj_smtr projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_rj_riodejaneiro_onibus_gps.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_rj_riodejaneiro_onibus_gps project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.rj_smtr.br_rj_riodejaneiro_onibus_gps.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_rj_riodejaneiro_onibus_gps project
    """

    BASE_URL = (
        "http://ccomobility.com.br/WebServices/Binder/WSConecta/EnvioInformacoesIplan?"
    )
    SPPO_DATASET_ID = "br_rj_riodejaneiro_onibus_gps"
    SPPO_TABLE_ID = "registros"
    SPPO_SECRET_PATH = "sppo_api"
