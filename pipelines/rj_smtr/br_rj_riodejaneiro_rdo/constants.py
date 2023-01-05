# -*- coding: utf-8 -*-
"""
Constant values for the rj_smtr projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_rj_riodejaneiro_rdo.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_rj_riodejaneiro_rdo project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_rj_riodejaneiro_rdo project
    """

    RDO_PRE_TREATMENT_CONFIG = {
        "SPPO": {
            "RDO": {
                "reindex_columns": [
                    "operadora",
                    "linha",
                    "data_transacao",
                    "tarifa_valor",
                    "gratuidade_idoso",
                    "gratuidade_especial",
                    "gratuidade_estudante_federal",
                    "gratuidade_estudante_estadual",
                    "gratuidade_estudante_municipal",
                    "universitario",
                    "gratuito_rodoviario",
                    "buc_1a_perna",
                    "buc_2a_perna",
                    "buc_receita",
                    "buc_supervia_1a_perna",
                    "buc_supervia_2a_perna",
                    "buc_supervia_receita",
                    "buc_van_1a_perna",
                    "buc_van_2a_perna",
                    "buc_van_receita",
                    "buc_vlt_1a_perna",
                    "buc_vlt_2a_perna",
                    "buc_vlt_receita",
                    "buc_brt_1a_perna",
                    "buc_brt_2a_perna",
                    "buc_brt_3a_perna",
                    "buc_brt_receita",
                    "buc_inter_1a_perna",
                    "buc_inter_2a_perna",
                    "buc_inter_receita",
                    "buc_barcas_1a_perna",
                    "buc_barcas_2a_perna",
                    "buc_barcas_receita",
                    "buc_metro_1a_perna",
                    "buc_metro_2a_perna",
                    "buc_metro_receita",
                    "cartao",
                    "receita_cartao",
                    "especie_passageiro_transportado",
                    "especie_receita",
                    "registro_processado",
                    "data_processamento",
                    "linha_rcti",
                ],
                "divide_columns": "",
                "reorder_columns": "",
            },
            "RHO": {
                "reindex_columns": [
                    "linha",
                    "data_transacao",
                    "hora_transacao",
                    "total_gratuidades",
                    "total_pagantes_especie",
                    "total_pagantes_cartao",
                    "registro_processado",
                    "data_processamento",
                    "operadora",
                    "linha_rcti",
                ],
                "divide_columns": "",
                "reorder_columns": "",
            },
        },
        "STPL": {
            "RDO": {
                "reindex_columns": [
                    "operadora",
                    "linha",
                    "tarifa_valor",
                    "data_transacao",
                    "gratuidade_idoso",
                    "gratuidade_especial",
                    "gratuidade_estudante_federal",
                    "gratuidade_estudante_estadual",
                    "gratuidade_estudante_municipal",
                    "universitario",
                    "buc_1a_perna",
                    "buc_2a_perna",
                    "buc_receita",
                    "buc_supervia_1a_perna",
                    "buc_supervia_2a_perna",
                    "buc_supervia_receita",
                    "buc_van_1a_perna",
                    "buc_van_2a_perna",
                    "buc_van_receita",
                    "buc_brt_1a_perna",
                    "buc_brt_2a_perna",
                    "buc_brt_3a_perna",
                    "buc_brt_receita",
                    "buc_inter_1a_perna",
                    "buc_inter_2a_perna",
                    "buc_inter_receita",
                    "buc_metro_1a_perna",
                    "buc_metro_2a_perna",
                    "buc_metro_receita",
                    "cartao",
                    "receita_cartao",
                    "especie_passageiro_transportado",
                    "especie_receita",
                    "registro_processado",
                    "data_processamento",
                    "linha_rcti",
                    "codigo",
                ],
                "reorder_columns": {"tarifa_valor": 3, "data_transacao": 2},
                "divide_columns": [
                    "tarifa_valor",
                    "buc_receita",
                    "buc_supervia_receita",
                    "buc_van_receita",
                    "buc_brt_receita",
                    "buc_inter_receita",
                    "buc_metro_receita",
                    "receita_cartao",
                    "especie_receita",
                ],
            },
            "RHO": {
                "reindex_columns": [
                    "operadora",
                    "linha",
                    "data_transacao",
                    "hora_transacao",
                    "total_gratuidades",
                    "total_pagantes",
                    "codigo",
                ],
                "divide_columns": "",
                "reorder_columns": "",
            },
        },
    }
