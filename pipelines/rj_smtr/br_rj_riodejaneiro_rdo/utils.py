# -*- coding: utf-8 -*-
"""
General purpose functions for the br_rj_riodejaneiro_rdo project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_rj_riodejaneiro_rdo.
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
# from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.utils import foo
# foo()
# ```
#
###############################################################################
from datetime import timedelta
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.implicit_ftp import ImplicitFtpTls
from pipelines.utils.utils import get_vault_secret
from pipelines.rj_smtr.constants import constants


def connect_ftp():
    """Connect to FTP

    Returns:
        ImplicitFTP_TLS: ftp client
    """
    ftp_data = get_vault_secret(constants.FTPS_SECRET_PATH.value)["data"]
    ftp_client = ImplicitFtpTls()
    ftp_client.connect(host=ftp_data["host"], port=int(ftp_data["port"]))
    ftp_client.login(user=ftp_data["username"], passwd=ftp_data["pwd"])
    ftp_client.prot_p()
    return ftp_client


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


def generate_ftp_schedules(
    interval_minutes: int, label: str = emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value
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
                    parameter_defaults={
                        "transport_mode": mode,
                        "report_type": report,
                        "table_id": build_table_id(mode=mode, report_type=report),
                    },
                    labels=[label],
                )
            )
    return clocks
