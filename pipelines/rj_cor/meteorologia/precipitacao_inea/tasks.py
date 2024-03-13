# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Tasks for precipitacao_inea
"""
from datetime import timedelta
from pathlib import Path
from typing import Union, Tuple

import numpy as np
import pandas as pd
import pendulum
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped, Failed
from pipelines.constants import constants
from pipelines.utils.utils import (
    log,
    parse_date_columns,
    save_updated_rows_on_redis,
    to_partitions,
)


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_data() -> pd.DataFrame:
    """
    Download data from API
    """

    estacoes = {
        "1": "225543320",  # Campo Grande
        "2": "BE70E166",  # Capela Mayrink
        "3": "225543250",  # Eletrobras
        "4": "2243088",  # Realengo
        "5": "225443130",  # Sao Cristovao
    }

    dataframe = pd.DataFrame()
    for key, value in estacoes.items():
        url = f"http://200.20.53.8/alertadecheias/{value}.xlsx"
        dataframe_temp = pd.read_excel(url)
        dataframe_temp["id_estacao"] = key
        dataframe = pd.concat([dataframe, dataframe_temp])
    return dataframe


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treat_data(
    dataframe: pd.DataFrame, dataset_id: str, table_id: str, mode: str = "dev"
) -> Tuple[pd.DataFrame, bool]:
    """
    Rename cols and filter data using Redis
    """

    dataframe["data_medicao"] = (
        pd.to_datetime(dataframe.Data, format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
        + " "
        + dataframe["Hora"]
    )

    rename_cols = {
        "Chuva Último dado": "acumulado_chuva_15_min",
        " Chuva Acumulada 1H": "acumulado_chuva_1_h",
        " Chuva Acumulada 4H": "acumulado_chuva_4_h",
        " Chuva Acumulada 24H": "acumulado_chuva_24_h",
        " Chuva Acumulada 96H": "acumulado_chuva_96_h",
        " Chuva Acumulada 30D": "acumulado_chuva_30_d",
        " Último Nível": "altura_agua",
    }
    dataframe.rename(columns=rename_cols, inplace=True)

    # replace all "Dado Nulo" to nan
    dataframe.replace({"Dado Nulo": np.nan}, inplace=True)

    # Eliminate where the id_estacao is the same keeping the smallest one
    dataframe.sort_values(
        ["id_estacao", "data_medicao"] + list(rename_cols.values()), inplace=True
    )
    dataframe.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    date_format = "%Y-%m-%d %H:%M:%S"
    # dataframe["data_medicao"] = dataframe["data_medicao"].dt.strftime(date_format)

    log(f"Dataframe before comparing with last data saved on redis {dataframe.head()}")
    log(f"Dataframe before comparing {dataframe[dataframe['id_estacao']=='1']}")

    dataframe = save_updated_rows_on_redis(
        dataframe,
        dataset_id,
        table_id,
        unique_id="id_estacao",
        date_column="data_medicao",
        date_format=date_format,
        mode=mode,
    )

    log(f"Dataframe after comparing with last data saved on redis {dataframe.head()}")
    log(f"Dataframe after comparing {dataframe[dataframe['id_estacao']=='1']}")

    # If df is empty stop flow
    if dataframe.shape[0] == 0:
        skip_text = "No new data available on API"
        log(skip_text)
        raise ENDRUN(state=Skipped(skip_text))

    pluviometric_cols = [
        "id_estacao",
        "data_medicao",
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
        "acumulado_chuva_30_d",
    ]
    fluviometric_cols = ["data_medicao", "altura_agua"]

    dfr_pluviometric = dataframe.loc[
        dataframe["altura_agua"] == "Estação pluviométrica", pluviometric_cols
    ].copy()
    dfr_fluviometric = dataframe.loc[
        dataframe["altura_agua"] != "Estação pluviométrica", fluviometric_cols
    ].copy()

    # Replace all values bigger than 10000 on altura_agua to nan
    dfr_fluviometric.loc[
        dfr_fluviometric["altura_agua"] > 10000, "altura_agua"
    ] = np.nan

    dfr_fluviometric["id_reservatorio"] = 1
    dfr_fluviometric["tipo_reservatorio"] = "rio"

    fluviometric_cols_order = [
        "id_reservatorio",
        "data_medicao",
        "tipo_reservatorio",
        "altura_agua",
    ]
    dfr_fluviometric = dfr_fluviometric[fluviometric_cols_order].copy()

    return dfr_pluviometric, dfr_fluviometric


@task
def save_data(dataframe: pd.DataFrame, folder_name: str = None) -> Union[str, Path]:
    """
    Save data on a csv file to be uploaded to GCP
    """

    prepath = Path("/tmp/precipitacao")
    if folder_name:
        prepath = Path("/tmp/precipitacao") / folder_name
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data_medicao"
    dataframe, partitions = parse_date_columns(dataframe, partition_column)
    current_time = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")

    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=current_time,
    )
    log(f"[DEBUG] Files saved on {prepath}")
    return prepath


@task
def check_for_new_stations(
    dataframe: pd.DataFrame,
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Check if the updated stations are the same as before.
    If not, consider flow as failed and call attention to
    add this new station on estacoes_cemaden.
    I can't automatically update this new station, because
    I couldn't find a url that gives me the lat and lon for
    all the stations.
    """

    stations_before = [
        "1",
        "2",
        "3",
        "4",
        "5",
    ]
    new_stations = [
        i for i in dataframe.id_estacao.unique() if str(i) not in stations_before
    ]
    if len(new_stations) != 0:
        message = f"New station identified. You need to update INEA\
              estacoes_inea adding station(s) {new_stations}: \
              {dataframe[dataframe.id_estacao.isin(new_stations)]}  "
        log(message)
        raise ENDRUN(state=Failed(message))
