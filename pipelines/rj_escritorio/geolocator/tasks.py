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
import prefect
from prefect import task

from pipelines.rj_escritorio.geolocator.utils import geolocator
from pipelines.utils.utils import log


# Importando os dados
@task
def importa_bases_e_chamados() -> list:
    """
    Importa a base de endereços completa e os endereços dos chamados que entraram no dia anterior.
    """
    query = """
    SELECT * FROM `rj-escritorio-dev.dados_mestres_staging.enderecos_geolocalizados`
    """
    base_enderecos_atual = bd.read_sql(
        query, billing_project_id="rj-escritorio-dev", from_file=True
    )
    enderecos_conhecidos = base_enderecos_atual["endereco_completo"]

    d1 = prefect.context.get("yesterday")  # pylint: disable=invalid-name
    d2 = prefect.context.get("today")  # pylint: disable=invalid-name
    query_2 = f"""
    with teste as (
    SELECT
    'Brasil' pais,
    'RJ' estado,
    'Rio de Janeiro' municipio,
    no_bairro bairro,
    id_logradouro,
    no_logradouro logradouro,
    ds_endereco_numero numero_porta,
    CONCAT(no_logradouro, ' ', ds_endereco_numero, ', ', no_bairro,
         ', ', 'Rio de Janeiro, RJ, Brasil') endereco_completo
    FROM `rj-segovi.administracao_servicos_publicos_1746_staging.chamado`
        WHERE no_logradouro IS NOT NULL
        AND dt_inicio >= '{d1}' AND dt_inicio <= '{d2}'
        ORDER BY id_chamado ASC
    )
    select distinct
        endereco_completo, pais, estado, municipio,
        bairro, id_logradouro, logradouro, numero_porta
        from teste
    """
    chamados_ontem = bd.read_sql(
        query_2, billing_project_id="rj-escritorio-dev", from_file=True
    )
    enderecos_ontem = chamados_ontem["endereco_completo"]

    return [enderecos_conhecidos, enderecos_ontem, chamados_ontem, base_enderecos_atual]


# Pegando apenas os endereços NOVOS que entraram ontem
@task(nout=2)
def enderecos_novos(lista_enderecos: list) -> Tuple[pd.DataFrame, bool]:
    """
    Retorna apenas os endereços não catalogados que entraram no dia anterior
    """
    novos_enderecos = lista_enderecos[1][~lista_enderecos[1].isin(lista_enderecos[0])]
    base_enderecos_novos = lista_enderecos[2][
        lista_enderecos[2]["endereco_completo"].isin(novos_enderecos)
    ]

    log(f"Quantidade de endereços novos: {len(novos_enderecos)}")

    possui_enderecos_novos = len(novos_enderecos) > 0

    return base_enderecos_novos, possui_enderecos_novos


# Geolocalizando
@task
def geolocaliza_enderecos(base_enderecos_novos: pd.DataFrame) -> pd.DataFrame:
    """
    Geolocaliza todos os novos endereços que entraram no dia anterior.
    """
    start_time = time.time()
    coordenadas = base_enderecos_novos["endereco_completo"].apply(
        lambda x: pd.Series(geolocator(x), index=["lat", "long"])
    )

    log(f"--- {(time.time() - start_time)} seconds ---")

    try:
        base_enderecos_novos["latitude"] = coordenadas["lat"]
        base_enderecos_novos["longitude"] = coordenadas["long"]
    except:  # pylint: disable=bare-except
        log("Não foram identificados chamados abertos no dia anterior.")

    return base_enderecos_novos


# @task
# def get_today() -> str:
#     """
#     Pega o endereço de hoje no formato YYYY-MM-DD
#     """
#     today = prefect.context.get("today")
#     return today


# Adicionando os endereços novos geolocalizados na base de endereços que já possuímos
@task
def cria_csv(base_enderecos_novos: pd.DataFrame) -> Union[str, Path]:
    """
    Une os endereços previamente catalogados com os novos e cria um csv.
    """

    # Converte "id_logradouro" e "numero_porta" em inteiro para retirar zero dos decimais
    base_enderecos_novos[["id_logradouro", "numero_porta"]] = (
        base_enderecos_novos[["id_logradouro", "numero_porta"]]
        .astype(float)
        .astype(int)
    )

    # Hora atual no formato YYYYMMDDHHmm para criar partições
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
