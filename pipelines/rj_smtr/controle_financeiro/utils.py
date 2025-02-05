# -*- coding: utf-8 -*-
"""
General purpose functions for rj_smtr.controle_financeiro
"""

from datetime import date, timedelta
import pandas as pd


def get_date_ranges(last_date: str, pending_dates: list[str]) -> list[dict]:
    """
    Create date ranges for consecutive dates

    Args:
        dates (list[str]): A list of strings representing dates (YYYY-mm-dd)
    Returns:
        list[dict]: The list of date ranges
    """
    initial_date = date.fromisoformat(last_date) + timedelta(days=1)
    final_date = date.today()

    if len(pending_dates) == 0:
        return [
            {
                "dt_inicio": initial_date.isoformat(),
                "dt_fim": final_date.isoformat(),
            }
        ]

    new_dates = [
        d.date().isoformat()
        for d in pd.date_range(
            initial_date,
            final_date,
        )
    ]

    dates = sorted(list(set(new_dates + pending_dates)))

    initial_date = dates[0]
    final_date = dates[0]

    ranges = []
    if len(dates) > 1:
        for i in range(1, len(dates)):
            current_date = dates[i]
            last_date = dates[i - 1]

            if date.fromisoformat(current_date) == date.fromisoformat(
                last_date
            ) + timedelta(days=1):
                final_date = current_date
            else:
                ranges.append({"dt_inicio": initial_date, "dt_fim": final_date})
                initial_date = final_date = current_date

    ranges.append({"dt_inicio": initial_date, "dt_fim": final_date})
    return ranges
