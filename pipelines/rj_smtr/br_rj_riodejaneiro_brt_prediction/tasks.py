# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_brt_prediction
"""

from prefect import task
import os
import pandas as pd
import numpy as np
from datetime import date

# import pyarrow.parquet as pq
import dask.dataframe as dd

from pipelines.utils.tasks import log
from pipelines.rj_smtr.br_rj_riodejaneiro_brt_prediction.constants import constants

from pipelines.rj_smtr.br_rj_riodejaneiro_brt_prediction.utils import (
    bigquery_client,
    query_gcp,
    transform_datetime_columns,
    map_direction,
    map_direction_inverse,
    distancias_ate_ponto,
    get_stop_times,
    add_stops_info,
    add_routes_info,
    get_initial_brt_stops,
    get_frequencies,
    get_shapes_and_stops,
    get_intervalos,
    get_day_hour_index,
    fill_hours,
    recover_historical_brt_service_register,
    get_trip_map,
    drop_undesired_gps,
    add_direction,
    add_trip_id,
    get_important_columns,
    get_stops,
    get_estacao_parado,
    chegada_estacao,
    tempo_percursos,
    get_intervals_by_trip,
    drop_outliers,
)


@task
def preprocess_brt_stops():
    """Preprocesses BRT stops data, merging with route and additional info"""

    log("Iniciando Tratamento - BRT Stops")

    df_stops_brt_info = get_stop_times().pipe(add_stops_info).pipe(add_routes_info)

    df_stops_brt_info.to_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/preprocess/brt_stops.csv",
        index=False,
    )

    log("Tratamento Finalizado - BRT Stops")


@task
def preprocess_frequencies_info():
    """Preprocesses BRT frequencies data, generating a departure time table"""

    log("Iniciando Tratamento - Frequencies Info")

    df_initial_brt_stops = get_initial_brt_stops()
    df_frequencies = get_frequencies()

    df_frequencies_info = df_frequencies.merge(df_initial_brt_stops, on="trip_id")

    itinerary_names = df_frequencies_info.apply(
        lambda row: map_direction_inverse(row["trip_id"])
        + " - "
        + row["route_short_name"]
        + " "
        + row["stop_name"].upper(),
        axis=1,
    )

    df_official_frequencies = pd.DataFrame(
        {
            "longitude": df_frequencies_info["stop_lon"],
            "latitude": df_frequencies_info["stop_lat"],
            "vei_nro_gestor": np.nan,
            "direcao": np.nan,
            "velocidade": np.nan,
            "inicio_viagem": df_frequencies_info["start_time"],
            "linha": df_frequencies_info["route_short_name"],
            "nomeLinha": df_frequencies_info["route_long_name"],
            "nomeItinerario": itinerary_names,
            "comunicacao": df_frequencies_info["start_time"],
        }
    )

    df_official_frequencies.to_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/preprocess/departure_times.csv",
        index=False,
    )

    log("Tratamento Finalizado - Frequencies Info")


@task
def preprocess_shapes_stops():
    """Merges shapes and stops data"""

    log("Iniciando Tratamento - Shapes Stops")

    df_shapes = pd.read_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/gtfs-brt-treated/shapes.csv"
    ).rename(columns={"shape_pt_lat": "latitude", "shape_pt_lon": "longitude"})

    df_brt_stops = pd.read_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/preprocess/brt_stops.csv"
    )

    df_shapes_with_stops = (
        df_shapes.groupby("shape_id")
        .apply(get_shapes_and_stops, df_brt_stops)
        .reset_index(drop=True)
    )

    df_shapes_with_stops.to_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/preprocess/shapes_with_stops.csv",
        index=False,
    )

    log("Tratamento Finalizado - Shapes Stops")


@task
def recover_historical_brt_registers(first_date: date, end_date: date):
    """Recovers brt historical registers from BigQuery, treat and saves"""

    # TODO: Aqui vamos trocar por uma lista fixa dos servicos!!!!!
    query_servicos = f"""
        SELECT DISTINCT servico
        FROM `rj-smtr-dev.br_rj_riodejaneiro_brt_gps.registros_desaninhada`
        WHERE data >= "{first_date}" and data <= "{end_date}"
    """

    log(query_servicos)

    response = query_gcp(bigquery_client, query_servicos)
    servicos = [resp["servico"] for resp in response]

    trip_mapper = get_trip_map()

    log(f"Iniciando Tratamento dos Registros: {servicos}\n")

    for servico in servicos:
        log(f"Coletando dados do serviço {servico}")

        df_gps = (
            recover_historical_brt_service_register(servico, first_date, end_date)
            .pipe(drop_undesired_gps)
            .pipe(add_direction)
            .pipe(add_trip_id, trip_mapper)
            .pipe(get_important_columns)
        )

        df_gps.to_parquet(
            f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/brt-register/{servico}.parquet",
            engine="fastparquet",
        )

    log("Tratamento Finalizado - Registros")


@task
def extract_historical_brt_intervals():
    """Based on historical BRT registers, extracts each real time for interval between stops"""

    # ATENCAO: Aqui tive que quebrar em um for porque localmente nao consigo puxar todos os dados
    # Poderiamos usar o spark para tratar os dados.

    df_registros_brt = dd.read_parquet(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/brt-register/"
    )
    unique_tripid = df_registros_brt["trip_id"].unique().compute()

    log("Iniciando Processamento - Intervalos Historicos")

    log(f"Trip ids identificados {unique_tripid.tolist()}")

    df_trechos = pd.DataFrame()
    df_stops_brt = get_stops()

    for tripid in unique_tripid[:5]:
        log(f"Coletando trechos de {tripid}...")

        if tripid is None:
            continue

        try:
            df_registros_tripid = df_registros_brt[
                df_registros_brt["trip_id"] == tripid
            ].compute()

            df_tempo_percursos = get_intervals_by_trip(
                df_registros_tripid, df_stops_brt
            )

            df_trechos = pd.concat(
                [df_trechos, df_tempo_percursos], axis=0, ignore_index=True
            )
        except Exception as e:
            log("Erro na coleta!", e)

    log("Finalizada a coleta dos intervalos por trips. Iniciando limpeza...")

    df_tempo_percursos_limpo = (
        df_trechos.groupby(["stop_id_origem", "stop_id_destino"])
        .apply(drop_outliers)
        .reset_index(drop=True)
    )

    df_tempo_percursos_limpo.to_parquet(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/intervalo_trechos/intervalo_trechos.parquet",
        engine="fastparquet",
    )

    log("Finalizado Processamento - Intervalos Historicos")


@task
def create_median_model():
    log("Iniciando a Criação do Modelo Mediana")

    df_intervalos = get_intervalos()

    modelo = (
        df_intervalos.groupby(
            ["stop_id_origem", "stop_id_destino", "dia_da_semana", "hora"]
        )["delta_tempo"]
        .median()
        .sort_index()
    )

    day_hour_index = get_day_hour_index()
    modelo_filled = (
        modelo.reset_index()
        .groupby(["stop_id_origem", "stop_id_destino"])
        .apply(fill_hours, ["dia_da_semana", "hora"], day_hour_index)
        .reset_index(["stop_id_origem", "stop_id_destino"])
    )

    modelo_filled["delta_tempo_minuto"] = modelo_filled.delta_tempo.dt.seconds.div(60.0)

    modelo_filled.to_parquet(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/modelos/mediana/geral.parquet",
        engine="fastparquet",
    )

    log("Modelo de Mediana Finalizado")
