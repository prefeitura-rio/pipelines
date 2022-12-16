# -*- coding: utf-8 -*-
"""
General purpose functions for the br_rj_riodejaneiro_rdo project
"""

from datetime import timedelta, datetime
from pytz import timezone
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.constants import constants


def build_table_id(mode: str, report_type: str):
    """Build table_id based on which table is the target
    of current flow run

    Args:
        mode (str): SPPO or STPL
        report_type (str): RHO or RDO

    Returns:
        str: table_id
    """
    if mode == "SPPO":
        if report_type == "RDO":
            table_id = constants.SPPO_RDO_TABLE_ID.value
        else:
            table_id = constants.SPPO_RHO_TABLE_ID.value
    if mode == "STPL":
        # slice the string to get rid of V at end of
        # STPL reports filenames
        if report_type[:3] == "RDO":
            table_id = constants.STPL_RDO_TABLE_ID.value
        else:
            table_id = constants.STPL_RHO_TABLE_ID.value
    return table_id


def merge_file_info_and_errors(files: list, errors: list):
    """

    Args:
        files (list): List of dicts
        errors (list): list of errors

    Returns:
        list: containing dicts with updated error
    """
    for i, file in enumerate(files):
        file["error"] = errors[i]
    return files


def generate_ftp_schedules(
    interval_minutes: int, label: str = emd_constants.RJ_SMTR_AGENT_LABEL.value
):
    """Generates IntervalClocks with the parameters needed to capture
    each report.

    Args:
        interval_minutes (int): interval which this flow will be run.
        label (str, optional): Prefect label, defines which agent to use when launching flow run.
        Defaults to emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value.

    Returns:
        List(IntervalClock): containing the clocks for scheduling runs
    """
    modes = ["SPPO", "STPL"]
    reports = ["RDO", "RHO"]
    clocks = []
    for mode in modes:
        for report in reports:
            clocks.append(
                IntervalClock(
                    interval=timedelta(minutes=interval_minutes),
                    start_date=datetime(
                        2022, 12, 16, 5, 0, tzinfo=timezone(constants.TIMEZONE.value)
                    ),
                    parameter_defaults={
                        "transport_mode": mode,
                        "report_type": report,
                        "table_id": build_table_id(mode=mode, report_type=report),
                    },
                    labels=[label],
                )
            )
    return clocks
