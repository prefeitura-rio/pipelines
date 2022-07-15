# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

from datetime import datetime
from io import StringIO, BytesIO
from pathlib import Path
from zipfile import ZipFile

from prefect import task
import pandas as pd

import requests


from pipelines.rj_smtr.projeto_subsidio_sppo.utils import melt_by_direction


@task
def get_sheets(share_url: str):
    """Download google sheets to pandas DataFrame

    Args:
        share_url (str): share link generated on google drive
    Returns:
        pandas.core.DataFrame: sheets content
    """
    file_id = share_url.split("/")[-2]
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    data = requests.get(url)
    df = pd.read_csv(StringIO(data.content.decode("utf-8")))  # pylint: disable=C0103
    return df


@task
def get_and_extract_gtfs(share_url: str):
    """Get gtfs zip file and extract locally

    Args:
        share_url (str): share link generated on google drive

    Returns:
        str: path to the directory containing extracted files
    """
    file_id = share_url.split("/")[-2]
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    extract_path = Path(f"gtfs/data={datetime.now().date()}/")
    extract_path.mkdir(parents=True, exist_ok=True)
    content = BytesIO(requests.get(url).content)
    ZipFile(content).extractall(extract_path)
    return extract_path


@task
def get_quadro_horario(df, date):  # pylint: disable=C0103, W0613
    """Treatment for quadro_horario csv

    Args:
        df (pandas.core.DataFrame): data to treat
        date (str): date

    Returns:
        pandas.core.DataFrame: treated data
    """

    # corrige nome do servico
    df["servico"] = df["servico"].str.extract(r"([A-Z]+)").fillna("") + df[
        "servico"
    ].str.extract(r"([0-9]+)")

    id_vars = ["servico", "vista", "consorcio", "horario_inicio", "horario_fim"]

    # trata km por tipo dia
    replace = {
        "km_dia_util": "Dia Útil",
        "km_sabado": "Sabado",
        "km_domingo": "Domingo",
    }
    km_col = "distancia_total_planejada"
    km_day_cols = ["km_dia_util", "km_sabado", "km_domingo"]

    for col in km_day_cols:
        df[col] = df[col].str.replace(".", "").str.replace(",", ".").astype(float)

    # reajusta distancia dee fds
    # df["km_sabado"] = round(df["km_sabado"] * 0.8, 3)
    # df["km_domingo"] = round(df["km_domingo"] * 0.8, 3)

    df_km_day = (
        df[id_vars + km_day_cols]
        .melt(id_vars, var_name="tipo_dia", value_name=km_col)
        .dropna(subset=[km_col])
        .replace(replace)
    )

    # trata trips e extensao por sentido
    df_trip = melt_by_direction(df, id_vars, value_name="trip_id")
    df_km_trip = melt_by_direction(df, id_vars, value_name="extensao")

    df_trip["sentido"] = df_trip["trip_id"].apply(lambda x: x[10:11])
    df_trip = (
        df_trip.merge(df_km_trip, on=id_vars + ["aux"], how="inner")
        .drop(columns="aux")
        .rename(columns={"extensao": "distancia_planejada"})
    )

    df_trip["distancia_planejada"] = df_trip["distancia_planejada"].astype(float)

    # junta informacao de km por tipo dia
    df = df_trip.merge(df_km_day, on=id_vars, how="outer")
    # df["data_versao"] = date

    return df
