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
from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.implicit_ftp import ImplicitFtpTls
from pipelines.utils.utils import get_vault_secret
from pipelines.rj_smtr.constants import constants


def connect_ftp():
    """Connect to FTP

    Returns:
        ImplicitFTP_TLS: ftp client
    """
    ftp_data = get_vault_secret(constants.RDO_SECRET_PATH.value)["data"]
    ftp_client = ImplicitFtpTls()
    ftp_client.connect(host=ftp_data["host"], port=990)
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
