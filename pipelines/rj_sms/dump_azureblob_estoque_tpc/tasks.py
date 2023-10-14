# -*- coding: utf-8 -*-
"""
Tasks for dump_azublob_estoque_tpc
"""

import pandas as pd
from prefect import task
from pipelines.utils.utils import log


@task
def conform_csv_to_gcp(filepath: str):
    """
    Reads a CSV file from the given filepath, applies some data cleaning and formatting operations,
    and saves the resulting dataframe back to the same file in a different format.

    Args:
        filepath (str): The path to the CSV file to be processed.

    Returns:
        None
    """
    df = pd.read_csv(filepath, sep=";", keep_default_na=False)

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

    df.to_csv(filepath, index=False, sep=";", encoding="utf-8", quoting=0, decimal=".")

    log("CSV now conform")
