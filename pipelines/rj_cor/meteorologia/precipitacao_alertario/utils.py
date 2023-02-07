# -*- coding: utf-8 -*-
"""
Utils for precipitacao_alertario
"""
from typing import List, Tuple

import numpy as np
import pandas as pd


def parse_date_columns(
    dataframe: pd.DataFrame, partition_date_column: str
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Parses the date columns to the partition format. Reformatado para manter o formato utilizado
    quando os dados foram salvos pela primeira vez (ano, mes, dia).
    """
    ano_col = "ano"
    mes_col = "mes"
    data_col = "dia"
    cols = [ano_col, mes_col, data_col]
    for col in cols:
        if col in dataframe.columns:
            raise ValueError(f"Column {col} already exists, please review your model.")

    dataframe[partition_date_column] = dataframe[partition_date_column].astype(str)
    dataframe[data_col] = pd.to_datetime(
        dataframe[partition_date_column], errors="coerce"
    )

    dataframe[ano_col] = (
        dataframe[data_col]
        .dt.year.fillna(-1)
        .astype(int)
        .astype(str)
        .replace("-1", np.nan)
    )

    dataframe[mes_col] = (
        dataframe[data_col]
        .dt.month.fillna(-1)
        .astype(int)
        .astype(str)
        .replace("-1", np.nan)
    )

    dataframe[data_col] = (
        dataframe[data_col]
        .dt.day.fillna(-1)
        .astype(int)
        .astype(str)
        .replace("-1", np.nan)
    )

    return dataframe, [ano_col, mes_col, data_col]
