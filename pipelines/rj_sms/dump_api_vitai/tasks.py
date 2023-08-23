# -*- coding: utf-8 -*-
from prefect import task
import pandas as pd
from datetime import date


@task
def fix_payload_vitai(filepath: str):
    df = pd.read_csv(filepath, sep=";", keep_default_na=False, dtype="str")

    # remove caracteres que confundem o parser
    df["descricao"] = df.descricao.apply(lambda x: x.replace('"', ""))
    df["descricao"] = df.descricao.apply(lambda x: x.replace(",", ""))

    df.to_csv(filepath, index=False, sep=",", encoding="utf-8")

    return filepath
