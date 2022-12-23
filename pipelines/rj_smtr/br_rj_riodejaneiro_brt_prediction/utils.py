# -*- coding: utf-8 -*-
"""
General purpose functions for the br_rj_riodejaneiro_brt_prediction project
"""

import pandas as pd
import os
import numpy as np
from haversine import haversine

from google.oauth2 import service_account
from google.cloud import bigquery

from pipelines.rj_smtr.br_rj_riodejaneiro_brt_prediction.constants import constants
from pipelines.utils.tasks import log


# -------- PREPARATION -------- #

# ISSO DEVE SER MODIFICADO !!!!
credentials = service_account.Credentials.from_service_account_file(
    f"{constants.CURRENT_DIR.value}/Segredos/gcp-key.json"
)
bigquery_client = bigquery.Client(credentials=credentials)


# -------- GENERAL PURPOSE UTILS -------- #


def query_gcp(bigquery_client, query_str):
    query_job = bigquery_client.query(query_str)

    response = []
    for row in query_job:
        response.append(dict(row))

    return response


def transform_datetime_columns(df, columns):
    for col in columns:
        df[col] = pd.to_datetime(df[col])
    return df


def map_direction(itinerary_name):
    """Given an itinerary name, return if 'IDA' or 'VOLTA'"""

    if itinerary_name.startswith("ITI IDA"):
        return "I"
    elif itinerary_name.startswith("ITI VOLTA"):
        return "V"
    else:
        return None


def map_direction_inverse(trip_id):
    """Given an trip_id, maps to the respective prefix"""

    trip_id_char = str(trip_id)[constants.TRIP_ID_INDICE_SENTIDO.value - 1]
    if trip_id_char == "I":
        return "ITI IDA"
    elif trip_id_char == "V":
        return "ITI VOLTA"
    else:
        return None


def distancias_ate_ponto(lista_coords, ponto_coord):
    """Calcula a distancia por haversine de uma lista de coordenadas (em tuplas) e um ponto"""
    return pd.Series(
        [haversine(coord, ponto_coord, unit="m") for coord in lista_coords]
    )


# -------- PREPROCESS BRT UTILS TASK -------- #


def get_stop_times():
    """Recupera dados da sequencia de stops para cada tripid"""

    return pd.read_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/gtfs-brt-treated/stop_times.csv"
    )


def add_stops_info(df_stops_brt):
    """Recupera dados relacionados a cada uma das stops, como lat long.
    E adiciona aos dados de sequencia de stops do brt essas infos."""

    df_stops_info = pd.read_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/gtfs-brt-treated/stops.csv"
    )
    df_stops_brt_info = df_stops_brt.merge(
        df_stops_info, on="stop_id", how="left"
    ).drop(
        [
            "timepoint",
            "stop_headsign",
            "location_type",
            "parent_station",
            "platform_code",
        ],
        axis=1,
    )
    return df_stops_brt_info


def add_routes_info(df_stops_brt):
    """Recupera dados relacionados a cada uma das rotas.
    E adiciona aos dados de sequencia de stops do brt essas infos."""

    df_routes = pd.read_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/gtfs-brt-treated/routes.csv"
    )
    df_stops_brt["route_id"] = df_stops_brt.trip_id.str[:10]
    df_stops_brt = df_stops_brt.merge(
        df_routes[["route_short_name", "route_id", "route_long_name"]],
        on="route_id",
        how="left",
    )
    return df_stops_brt


# -------- PREPROCESS FREQUENCIES TASK -------- #


def get_initial_brt_stops():
    """Recupera os dados preprocessados dos primeiros stops do brt_stops"""

    df_stops_brt = pd.read_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/preprocess/brt_stops.csv"
    )
    sel_columns = [
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "shape_dist_traveled",
        "stop_sequence",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "route_id",
        "route_short_name",
        "route_long_name",
    ]
    df_stops_brt = df_stops_brt.loc[:, sel_columns]
    return df_stops_brt[df_stops_brt.stop_sequence == 1]


def get_frequencies(sel_columns=["trip_id", "start_time"]):
    """Recupera dados das frequencias diarias dos servicos"""

    df_frequencies = pd.read_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/gtfs-brt-treated/frequencies.csv"
    )
    return df_frequencies.loc[:, sel_columns]


# -------- PREPROCESS SHAPES AND STOPS TASK -------- #


def get_shapes_and_stops(sel_df_shapes, df_stops_info):
    """Para cada um dos shapes juntamos com as informações
    de stops e com informações sobre a rota"""

    sel_df_shapes = sel_df_shapes.copy()
    shape_id = sel_df_shapes.iloc[0].shape_id

    sel_df_stops = df_stops_info[df_stops_info.trip_id == shape_id].rename(
        columns={"stop_lat": "latitude", "stop_lon": "longitude"}
    )

    df_shapes_stops = pd.concat(
        [
            sel_df_stops[
                [
                    "trip_id",
                    "stop_id",
                    "stop_name",
                    "latitude",
                    "longitude",
                    "shape_dist_traveled",
                    "stop_sequence",
                    "route_id",
                    "route_short_name",
                    "route_long_name",
                ]
            ],
            sel_df_shapes[
                ["shape_id", "latitude", "longitude", "shape_dist_traveled"]
            ].rename(columns={"shape_id": "trip_id"}),
        ],
        axis=0,
    ).sort_values(["shape_dist_traveled", "stop_id"])

    df_shapes_stops[["previous_stop_id", "previous_stop_name"]] = df_shapes_stops[
        ["stop_id", "stop_name"]
    ].ffill()
    df_shapes_stops[["next_stop_id", "next_stop_name"]] = (
        df_shapes_stops[["stop_id", "stop_name"]].bfill().shift(-1)
    )
    df_shapes_stops = df_shapes_stops[
        ~(df_shapes_stops["next_stop_id"].isna() & df_shapes_stops["stop_id"].isna())
    ]  # remove os shapes nulos
    df_shapes_stops = df_shapes_stops[
        ~(
            df_shapes_stops["previous_stop_id"].isna()
            & df_shapes_stops["stop_id"].isna()
        )
    ]  # remove os shapes nulos

    return df_shapes_stops


# -------- EXTRACT BRT HISTORICAL REGISTER TASK -------- #


def drop_undesired_gps(df):
    return df[df.sentido.str.startswith("ITI")]


def add_direction(df_gps):
    """Derives direction from 'sentido' column"""
    df_gps["direction"] = df_gps["sentido"].apply(map_direction)
    return df_gps


def get_important_columns(df_gps):
    colunas_importantes = ["id_veiculo", "servico", "latitude", "longitude"]
    df_gps = df_gps.dropna(subset=colunas_importantes)
    return df_gps


def get_trip_map():
    """Cria um df mapeador com colunas [linha, tripid, direction]"""
    df_trips = pd.read_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/gtfs-brt-treated/trips.csv"
    )
    df = df_trips.loc[:, ["trip_short_name", "trip_id"]]
    df["direction"] = [
        trip_id[constants.TRIP_ID_INDICE_SENTIDO.value - 1]
        for trip_id in df_trips["trip_id"]
    ]
    return df


def add_trip_id(df_gps, trip_mapper):
    """Using the trip mapper, recovers the trip id by the line and direction"""
    df_gps = df_gps.merge(
        trip_mapper,
        left_on=["servico", "direction"],
        right_on=["trip_short_name", "direction"],
        how="left",
    )
    return df_gps


def recover_historical_brt_service_register(brt_service_line, first_date, end_date):
    """Obter os registros historicos para uma linha do BRT"""

    query_gps_brt = f"""
        SELECT *
        FROM `rj-smtr-dev.br_rj_riodejaneiro_brt_gps.registros_desaninhada`
        WHERE data >= "{first_date}" and data <= "{end_date}"
        AND servico = '{brt_service_line}'
    """
    historical_gps_json = query_gcp(bigquery_client, query_gps_brt)

    df_historical_gps = (
        pd.DataFrame(historical_gps_json)
        .pipe(
            transform_datetime_columns, ["timestamp_gps", "timestamp_captura", "data"]
        )
        .sort_values("timestamp_gps")
    )

    return df_historical_gps


# -------- EXTRACT HISTORICAL BRT INTERVALS TASK -------- #


def get_stops():
    df_stops_brt = pd.read_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/preprocess/brt_stops.csv"
    )
    df_stops_brt = df_stops_brt[
        [
            "trip_id",
            "arrival_time",
            "departure_time",
            "stop_id",
            "shape_dist_traveled",
            "stop_sequence",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "route_id",
            "route_short_name",
            "route_long_name",
        ]
    ]
    return df_stops_brt


def get_estacao_parado(row, df_stops_trip):
    """
    O BRT trafega em média a 40km/h, que são ~11 m/s.
    Em um delay de 1 minuto temos ~700m trafegados e em 2 minutos ~1400m trafegados.
    Se acontecer tráfego a 60km/h, que são ~17 m/s.
    Em um delay de 1 minuto teremos ~1020m trafegados e em 2 minutos ~2040m trafegados.
    Porém, pudemos ver perda de estações quando o ônibus passava rápido com threshold de 400.
    Colocaremos um threshold bem alto porque posteriormente fazemos uma análise de qual ponto
    está mais perto dentro desse raio, então serve só pra filtragem dos mais perto mesmo
    """

    radius_threshold = constants.NEAR_STOP_THRESHOLD.value

    estacoes = df_stops_trip[["stop_id", "stop_lon", "stop_lat", "stop_sequence"]]
    pontos = estacoes.apply(lambda row: (row["stop_lat"], row["stop_lon"]), axis=1)
    try:
        distancias = distancias_ate_ponto(pontos, (row.latitude, row.longitude))
    except Exception as e:
        print(e)
        print(row)
        print((row.latitude, row.longitude))

    if distancias.min() < radius_threshold:
        stop_id = estacoes.iloc[distancias.idxmin()]["stop_id"]
        stop_index = estacoes.iloc[distancias.idxmin()]["stop_sequence"]
        is_stop_terminal = (
            True if (stop_index == 1 or stop_index == len(pontos)) else False
        )

        return pd.Series(
            {
                "stop_id": stop_id,
                "stop_sequence": stop_index,
                "distance_to_station": distancias.min(),
                "is_stop_terminal": is_stop_terminal,
            }
        )

    else:
        return pd.Series(dtype=str)


def chegada_estacao(df_gps_mesma_estacao):
    """Dado um trecho do df_gps com pontos na mesma estação, determinar a chegada e saida.
    Para pontos que nao sao terminais, pegaremos o de menor distancia ao ponto real
    e consideraremos chegada e saida.
    Para pontos terminais pegaremos o primeiro e ultimo existentes no trecho de pontos."""

    is_stop_terminal = df_gps_mesma_estacao.is_stop_terminal.iloc[0]
    stop_sequence = df_gps_mesma_estacao.stop_sequence.iloc[0]

    # TODO: Heuristica - possivel de melhorar
    if is_stop_terminal:
        if stop_sequence == 1:
            return df_gps_mesma_estacao.iloc[-1]  # ultimo (saindo)
        else:
            return df_gps_mesma_estacao.iloc[0]  # primeiro (chegando)

    else:
        ponto_mais_perto = df_gps_mesma_estacao.loc[
            df_gps_mesma_estacao.distance_to_station.idxmin()
        ]

        return ponto_mais_perto


def tempo_percursos(df_passagem_grupo):
    """Para um grupo de mesmo veiculo e servico, determinar as horas de
    chegada e saida de cada estacao da rota e obter o deltatempo"""

    df_passagem_grupo = df_passagem_grupo.sort_values("timestamp_gps").dropna(
        subset=["stop_sequence"]
    )

    # Obtem os indices do momento que os stop_sequence mudam de indice
    muda_stop_sequence = df_passagem_grupo.stop_sequence.diff().fillna(0) != 0
    muda_stop_sequence.iloc[0] = True
    index_new_sequence = df_passagem_grupo[muda_stop_sequence].index

    # Obtem a chegada e saida para cada estacao
    df_gps_chegada_saida = []
    for i in range(1, len(index_new_sequence)):
        current = index_new_sequence[i]
        before = index_new_sequence[i - 1]
        df_mesma_estacao = df_passagem_grupo.loc[before:current].iloc[:-1]
        linha_chegada = chegada_estacao(df_mesma_estacao)
        df_gps_chegada_saida.append(linha_chegada)
    current = index_new_sequence[len(index_new_sequence) - 1]
    linha_chegada = chegada_estacao(df_passagem_grupo.loc[current:current])
    df_gps_chegada_saida.append(linha_chegada)

    df_gps_chegada_saida = pd.DataFrame(df_gps_chegada_saida)

    if len(df_gps_chegada_saida) == 0:
        return pd.DataFrame()
    else:
        return pd.DataFrame(
            {
                "trip_id": df_gps_chegada_saida.trip_id.iloc[1:].values,
                "servico": df_gps_chegada_saida.iloc[0].servico,
                "direction": df_gps_chegada_saida.iloc[0].direction,
                "sentido": df_gps_chegada_saida.iloc[0].sentido,
                "stop_id_origem": df_gps_chegada_saida.stop_id.iloc[:-1].values,
                "stop_sequence_origem": df_gps_chegada_saida.stop_sequence.iloc[
                    :-1
                ].values,
                "stop_id_destino": df_gps_chegada_saida.stop_id.iloc[1:].values,
                "stop_sequence_destino": df_gps_chegada_saida.stop_sequence.iloc[
                    1:
                ].values,
                "tempo_saida": df_gps_chegada_saida.timestamp_gps.iloc[:-1].values,
                "tempo_chegada": df_gps_chegada_saida.timestamp_gps.iloc[1:].values,
                "delta_tempo": df_gps_chegada_saida.iloc[1:].reset_index().timestamp_gps
                - df_gps_chegada_saida.iloc[:-1].reset_index().timestamp_gps,
            }
        ).sort_values("tempo_saida")


def get_intervals_by_trip(df_registros_brt, df_stops_brt):
    # Obtendo a tripid atual
    trip_id = df_registros_brt.iloc[0].trip_id

    # Obtendo as stops especificas dessa trip
    df_stops_trip = df_stops_brt[(df_stops_brt.trip_id == trip_id)]

    # Obtendo os instantes que o onibus passa perto das estacoes
    df_passagem = pd.concat(
        [
            df_registros_brt,
            df_registros_brt.apply(get_estacao_parado, args=[df_stops_trip], axis=1),
        ],
        axis=1,
    ).dropna(subset=["stop_id"])

    # Obtendo os tempos entre trechos
    df_tempo_percursos = (
        df_passagem.groupby(["id_veiculo", "trip_id"])
        .apply(tempo_percursos)
        .reset_index(drop=True)
    )

    # Obtendo apenas as sequencias corretas
    df_tempo_percursos_correto = df_tempo_percursos[
        df_tempo_percursos.stop_sequence_destino
        - df_tempo_percursos.stop_sequence_origem
        == 1.0
    ]

    return df_tempo_percursos_correto


def drop_outliers(df):
    """Remove outliers do delta_tempo usando a técnica da distancia interquartil"""

    if len(df) > 1:
        iqr = df["delta_tempo"].dt.total_seconds().quantile(0.75) - df[
            "delta_tempo"
        ].dt.total_seconds().quantile(0.25)
        lim = (
            np.abs(
                (
                    df["delta_tempo"].dt.total_seconds()
                    - df["delta_tempo"].dt.total_seconds().median()
                )
                / iqr
            )
            < 1.5
        )
        df = df[lim]

    lim_horas = (
        df["delta_tempo"].dt.total_seconds()
        < constants.MAXIMUM_SECONDS_BETWEEN_STOPS.value
    )

    return df[lim_horas]


# -------- GENERATE MEDIAN MODEL TASK -------- #


def get_intervalos():
    """Obtem os dados de intervalo de tempo para cada trecho historico percorrido"""

    df_intervalos = pd.read_parquet(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/intervalo_trechos.parquet",
        engine="fastparquet",
    )

    df_intervalos["tempo_saida"] = pd.to_datetime(df_intervalos["tempo_saida"])
    df_intervalos["tempo_chegada"] = pd.to_datetime(df_intervalos["tempo_chegada"])
    df_intervalos["delta_tempo"] = pd.to_timedelta(df_intervalos["delta_tempo"])

    df_intervalos["hora"] = df_intervalos["tempo_saida"].dt.hour
    df_intervalos["dia_da_semana"] = df_intervalos.tempo_saida.dt.dayofweek
    df_intervalos["data"] = df_intervalos.tempo_saida.dt.date
    df_intervalos["horario"] = df_intervalos.tempo_saida.dt.time

    return df_intervalos


def get_day_hour_index():
    """Retorna um multindex com cada dia da semana e hora possivel"""

    return pd.MultiIndex.from_product(
        [range(0, 7), range(0, 24)], names=["dia_da_semana", "hora"]
    )


def fill_hours(df, cols, day_hour_index):
    """Preenche os dia-horas inexistentes no dataframe com o valor médio"""

    fill_value = df["delta_tempo"].mean()
    return (
        df.set_index(cols)
        .reindex(day_hour_index)[["delta_tempo"]]
        .fillna(fill_value)
        .reset_index()
    )
