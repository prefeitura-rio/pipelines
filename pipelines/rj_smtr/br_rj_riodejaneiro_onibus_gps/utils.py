# -*- coding: utf-8 -*-
"""
General purpose functions for the br_rj_riodejaneiro_onibus_gps project
"""
###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_rj_riodejaneiro_onibus_gps.
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
# from pipelines.rj_smtr.br_rj_riodejaneiro_onibus_gps.utils import foo
# foo()
# ```
#
###############################################################################
from datetime import timedelta
import pandas as pd
from pipelines.rj_smtr.constants import constants


def sppo_filters(frame: pd.DataFrame, version: int = 1, recapture: bool = False):
    """Apply filters to dataframe

    Args:
        frame (pd.DataFrame): Containing data captured from sppo
        api

    Returns:
        frame: Filtered input
    """
    if version == 1:
        filter_col = "timestamp_captura"
        time_delay = constants.GPS_SPPO_CAPTURE_DELAY_V1.value
    elif version == 2:
        filter_col = "datahoraenvio"
        time_delay = constants.GPS_SPPO_CAPTURE_DELAY_V2.value
    if recapture:
        server_mask = (frame["datahoraenvio"] - frame["datahoraservidor"]) <= timedelta(
            minutes=constants.GPS_SPPO_RECAPTURE_DELAY_V2.value
        )
        frame = frame[server_mask]

    mask = (frame[filter_col] - frame["datahora"]).apply(
        lambda x: timedelta(seconds=0) <= x <= timedelta(minutes=time_delay)
    )

    return frame[mask]
