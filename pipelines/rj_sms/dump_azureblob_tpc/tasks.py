# -*- coding: utf-8 -*-
from prefect import task
import pandas as pd
from pipelines.utils.utils import log
from datetime import date
from loguru import logger


@task
def conform_csv_to_gcp(filepath: str):
    df = pd.read_csv(filepath, sep=";", keep_default_na=False)

    # remove registros errados
    df = df[df.sku != ""]

    # remove caracteres que confundem o parser
    df["item_nome_longo"] = df.item_nome_longo.apply(lambda x: x.replace('"', ""))
    df["item_nome_longo"] = df.item_nome_longo.apply(lambda x: x.replace(",", ""))
    df["item_nome"] = df.item_nome_longo.apply(lambda x: x.replace(",", ""))

    # converte para valores numéricos
    df["volume"] = df.volume.apply(lambda x: float(x.replace(",", ".")))
    df["peso_bruto"] = df.peso_bruto.apply(lambda x: float(x.replace(",", ".")))
    df["qtd_dispo"] = df.qtd_dispo.apply(lambda x: float(x.replace(",", ".")))
    df["qtd_roma"] = df.qtd_roma.apply(lambda x: float(x.replace(",", ".")))

    # converte as validades
    df["validade"] = df.validade.apply(lambda x: x[:10])
    df["dt_situacao"] = df.dt_situacao.apply(
        lambda x: x[-4:] + "-" + x[3:5] + "-" + x[:2]
    )

    # add data da carga
    df["_data_carga"] = date.today()

    df.to_csv(filepath, index=False, sep=",", encoding="utf-8", quoting=0, decimal=".")

    logger.success("CSV now conform")
