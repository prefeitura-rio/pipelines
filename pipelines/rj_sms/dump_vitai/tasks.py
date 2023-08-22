# -*- coding: utf-8 -*-
from prefect import task
import pandas as pd
from datetime import date


@task
def fix_payload_vitai(filepath: str):
    
    df = pd.read_json(filepath)

    # remove caracteres que confundem o parser
    df["descricao"] = df.descricao.apply(lambda x: x.replace('"', ""))
    df["descricao"] = df.descricao.apply(lambda x: x.replace(",", ""))

    # adiciona a data de carga
    df["_data_carga"] = str(date.today())

    df.to_csv(filepath, index=False, sep=",", encoding="utf-8", quoting=0, decimal=".")

    return filepath