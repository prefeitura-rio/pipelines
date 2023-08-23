# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log
import pandas as pd
from datetime import date


@task
def conform_csv_to_gcp(input_path: str):
    df = pd.read_csv(input_path, sep=";", keep_default_na=False, dtype="str")

    # remove caracteres que confundem o parser
    df["descricao"] = df.descricao.apply(lambda x: x.replace('"', ""))
    df["descricao"] = df.descricao.apply(lambda x: x.replace(",", ""))

    df.to_csv(input_path, index=False, sep=",", encoding="utf-8")
    log("CSV now conform")

    return input_path
