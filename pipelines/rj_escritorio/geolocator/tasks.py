# -*- coding: utf-8 -*-
# flake8: noqa: E722
"""
Tasks for geolocator
"""

import os
from pathlib import Path
import time
from typing import Union, Tuple

import basedosdados as bd
import pandas as pd
import pendulum
from prefect import task

from pipelines.rj_escritorio.geolocator.utils import (
    geolocator,
    checar_point_pertence_cidade,
)
from pipelines.utils.utils import log


# Importando os dados
@task(nout=2)
def seleciona_enderecos_novos() -> Tuple[pd.DataFrame, bool]:
    """
    Seleciona base de endereços chamados que entraram no dia anterior e não estão geolocalizados.
    """
    # pylint: disable=W1401
    query = """
    WITH
    end_conhecidos AS (
        SELECT endereco_completo
        FROM `rj-escritorio-dev.dados_mestres_staging.enderecos_geolocalizados`
    ),
    chamados_ontem AS (
        SELECT
        'Brasil' pais,
        'RJ' estado,
        'Rio de Janeiro' municipio,
        no_bairro bairro,
        LPAD(SAFE_CAST(REGEXP_REPLACE(id_logradouro, r'\.0$', '') AS STRING), 6, '0') id_logradouro,
        no_logradouro logradouro,
        SAFE_CAST(SAFE_CAST(ds_endereco_numero AS INT) AS STRING) numero_porta,
        CONCAT(no_logradouro, ' ', SAFE_CAST(SAFE_CAST(ds_endereco_numero AS INT) AS STRING), ', ', no_bairro,
            ', ', 'Rio de Janeiro, RJ, Brasil') endereco_completo
        FROM `rj-segovi.adm_central_atendimento_1746_staging.chamado`
        WHERE no_logradouro IS NOT NULL
            AND CAST(dt_inicio AS TIMESTAMP) BETWEEN DATE_ADD(CAST(CURRENT_DATE() AS TIMESTAMP), INTERVAL -1 DAY) AND CURRENT_TIMESTAMP()
        )

    SELECT DISTINCT
    ch.endereco_completo, pais, estado, municipio,
    bairro, id_logradouro, logradouro, ch.numero_porta
    FROM chamados_ontem ch
    LEFT JOIN end_conhecidos ec ON ec.endereco_completo = ch.endereco_completo
    WHERE ec.endereco_completo IS NULL AND ch.endereco_completo IS NOT NULL
    """
    base_enderecos_novos = bd.read_sql(
        query, billing_project_id="rj-escritorio-dev", from_file=True
    )
    possui_enderecos_novos = base_enderecos_novos.shape[0] > 0

    return base_enderecos_novos.drop_duplicates(), possui_enderecos_novos


# Geolocalizando
@task
def geolocaliza_enderecos(base_enderecos_novos: pd.DataFrame) -> pd.DataFrame:
    """
    Geolocaliza todos os novos endereços que entraram no dia anterior
    e verifica se pontos pertencem a cidade do Rio de Janeiro.
    """
    start_time = time.time()
    coordenadas = base_enderecos_novos["endereco_completo"].apply(
        lambda x: pd.Series(geolocator(x), index=["lat", "long"])
    )

    coordenadas[["lat", "long"]] = coordenadas.apply(
        lambda x: checar_point_pertence_cidade(x.lat, x.long),
        axis=1,
        result_type="expand",
    )

    log(f"--- {(time.time() - start_time)} seconds ---")

    try:
        base_enderecos_novos["latitude"] = coordenadas["lat"]
        base_enderecos_novos["longitude"] = coordenadas["long"]
    except:  # pylint: disable=bare-except
        log("Não foram identificados chamados abertos no dia anterior.")

    return base_enderecos_novos


# Adicionando os endereços novos geolocalizados na base de endereços que já possuímos
@task
def cria_csv(base_enderecos_novos: pd.DataFrame) -> Union[str, Path]:
    """
    Une os endereços previamente catalogados com os novos e cria um csv.
    """

    # Fixa ordem das colunas
    cols_order = [
        "endereco_completo",
        "pais",
        "estado",
        "municipio",
        "bairro",
        "id_logradouro",
        "logradouro",
        "numero_porta",
        "latitude",
        "longitude",
    ]
    base_enderecos_novos[cols_order] = base_enderecos_novos[cols_order]

    # Hora atual no formato YYYY-MM-DD para criar partições
    current_day = pendulum.now("America/Sao_Paulo").strftime("%Y-%m-%d")

    ano = current_day[:4]
    mes = str(int(current_day[5:7]))
    partitions = os.path.join(
        f"ano_particao={ano}", f"mes_particao={mes}", f"data_particao={current_day}"
    )

    base_path = os.path.join(os.getcwd(), "tmp", "geolocator")
    partition_path = os.path.join(base_path, partitions)

    if not os.path.exists(partition_path):
        os.makedirs(partition_path)

    filename = os.path.join(partition_path, "base_enderecos.csv")
    log(f"File is saved on: {filename}")

    base_enderecos_novos.to_csv(
        filename,
        index=False,
    )
    return base_path
