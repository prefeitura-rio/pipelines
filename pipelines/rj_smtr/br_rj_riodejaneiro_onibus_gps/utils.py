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


def sppo_filters(frame: pd.DataFrame, version: int = 1):
    """Apply filters to dataframe

    Args:
        frame (pd.DataFrame): Containing data captured from sppo
        api

    Returns:
        frame: Filtered input
    """
    if version == 1:
        same_minute_mask = (frame["timestamp_captura"] - frame["datahora"]).apply(
            lambda x: timedelta(seconds=0) <= x <= timedelta(minutes=1)
        )
        return frame[same_minute_mask]
    if version == 2:
        sent_received_mask = (frame["datahoraenvio"] - frame["datahora"]).apply(
            lambda x: timedelta(seconds=0)
            <= x
            <= timedelta(minutes=constants.GPS_SPPO_CAPTURE_DELAY.value)
        )
        frame = frame[sent_received_mask]
        return frame
