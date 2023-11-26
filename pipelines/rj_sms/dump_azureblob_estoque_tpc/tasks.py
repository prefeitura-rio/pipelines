# -*- coding: utf-8 -*-
"""
Tasks for dump_azublob_estoque_tpc
"""

import pandas as pd
from prefect import task
from pipelines.utils.utils import log
from pipelines.rj_sms.dump_azureblob_estoque_tpc.constants import (
    constants as tpc_constants,
)


@task
def get_blob_path(blob_file: str):
    """
    Returns the blob path for the given blob file.

    Args:
        blob_file (str): The name of the blob file.

    Returns:
        str: The blob path for the given blob file.
    """
    return tpc_constants.BLOB_PATH.value[blob_file]

@task
def conform_csv_to_gcp(filepath: str, blob_file: str):
    """
    Reads a CSV file from the given filepath, applies some data cleaning and formatting operations,
    and saves the resulting dataframe back to the same file in a different format.

    Args:
        filepath (str): The path to the CSV file to be processed.

    Returns:
        None
    """
    log("Conforming CSV to GCP")

    # remove " from csv to avoid errors
    with open(filepath, 'r') as f:
        file_contents = f.read()

    file_contents = file_contents.replace('\"', '')

    with open(filepath, 'w') as f:
        f.write(file_contents)


    df = pd.read_csv(filepath, sep=";", dtype=str, keep_default_na=False)

    if blob_file == "posicao":

        # remove registros errados
        df = df[df.sku != ""]

        # converte para valores numéricos
        df["volume"] = df.volume.apply(lambda x: float(x.replace(",", ".")))
        df["peso_bruto"] = df.peso_bruto.apply(lambda x: float(x.replace(",", ".")))
        df["qtd_dispo"] = df.qtd_dispo.apply(lambda x: float(x.replace(",", ".")))
        df["qtd_roma"] = df.qtd_roma.apply(lambda x: float(x.replace(",", ".")))
        df["preco_unitario"] = df.preco_unitario.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )

        # converte as validades
        df["validade"] = df.validade.apply(lambda x: x[:10])
        df["dt_situacao"] = df.dt_situacao.apply(
            lambda x: x[-4:] + "-" + x[3:5] + "-" + x[:2]
        )
    elif blob_file == "pedidos":
        # converte para valores numéricos
        df["valor"] = df.valor.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )
        df["peso"] = df.peso.apply(lambda x: float(x.replace(",", ".")))
        df["volume"] = df.volume.apply(lambda x: float(x.replace(",", ".")))
        df["quantidade_peca"] = df.quantidade_peca.apply(lambda x: float(x.replace(",", ".")))
        df["valor_total"] = df.valor_total.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )
    elif blob_file == "recebimento":


        df["qt"] = df.qt.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )
        df["qt_fis"] = df.qt_fis.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )
        df["pr_unit"] = df.pr_unit.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )
        df["vl_merc"] = df.vl_merc.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )
        df["vl_total"] = df.vl_total.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )
        df["qt_rec"] = df.qt_rec.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )


    df.to_csv(filepath, index=False, sep=";", encoding="utf-8", quoting=0, decimal=".")

    log("CSV now conform")