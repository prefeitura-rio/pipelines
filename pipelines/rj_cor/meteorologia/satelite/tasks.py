# -*- coding: utf-8 -*-
"""
Tasks for emd
"""

import datetime as dt
import os
import re
from pathlib import Path
from typing import Tuple, Union

import numpy as np
import pendulum
from prefect import task
import s3fs

from pipelines.rj_cor.meteorologia.satelite.satellite_utils import (
    main,
    save_parquet,
    get_blob_with_prefix,
    download_blob,
)


@task()
def get_dates() -> str:
    """
    Task para obter o dia atual
    """
    current_time = pendulum.now("UTC").to_datetime_string()
    return current_time


@task(nout=5)
def slice_data(current_time: str) -> Tuple[str, str, str, str, str]:
    """
    slice data em ano. mês, dia, hora e dia juliano
    """
    ano = current_time[:4]
    mes = current_time[5:7]
    dia = current_time[8:10]
    hora = current_time[11:13]
    dia_juliano = dt.datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S").strftime("%j")
    return ano, mes, dia, hora, dia_juliano


@task(nout=2, max_retries=10, retry_delay=dt.timedelta(seconds=60))
def download(
    variavel: str,
    ano: str,
    dia_juliano: str,
    hora: str,
    band: str = None,
    redis_files: list = [],
    wait=None,
) -> Union[str, Path]:
    """
    Acessa o S3 e faz o download do primeiro arquivo da data-hora especificada
    """

    try:
        # Use the anonymous credentials to access public data
        s3_fs = s3fs.S3FileSystem(anon=True)

        # Get all files of GOES-16 data (multiband format) at this hour
        files = np.sort(
            np.array(
                s3_fs.find(f"noaa-goes16/ABI-L2-{variavel}/{ano}/{dia_juliano}/{hora}/")
                # s3_fs.find(f"noaa-goes16/ABI-L2-CMIPF/2022/270/10/OR_ABI-L2-CMIPF-M6C13_G16_s20222701010208_e20222701019528_c20222701020005.nc")
            )
        )
        # Mantém apenas arquivos de determinada banda
        if variavel == "CMIPF":
            # para capturar banda 13
            files = [f for f in files if bool(re.search("C" + band, f))]
        origem = "aws"
    except IndexError:
        bucket_name = "gcp-public-data-goes-16"
        partition_file = f"ABI-L2-{variavel}/{ano}/{dia_juliano}/{hora}/"
        files = get_blob_with_prefix(
            bucket_name=bucket_name, prefix=partition_file, mode="prod"
        )
        origem = "gcp"

    base_path = os.path.join(os.getcwd(), "data", "satelite", variavel[:-1], "input")

    if not os.path.exists(base_path):
        os.makedirs(base_path)
    print(">>>>>>>>>>>>>>> basepath", base_path)

    # Seleciona primeiro arquivo que não tem o nome salvo no redis
    print(
        ">>>>>>>>>>>>>>> files", files
    )  # OR_ABI-L2-CMIPF-M6C13_G16_s20222711500209_e20222711509528_c20222711510012.nc
    print(">>>>>>>>>>>>>>> redis_files", redis_files)
    files.sort()
    for file in files:
        filename = file.split("/")[-1]
        if filename not in redis_files:
            redis_files.append(filename)
            filename = os.path.join(base_path, filename)
            download_file = file
            print(">>>>>>>>>>>>>>> append redis_files", redis_files)
            break

    # Faz download da aws ou da gcp
    if origem == "aws":
        s3_fs.get(download_file, filename)
    else:
        download_blob(
            bucket_name=bucket_name,
            source_blob_name=download_file,
            destination_file_name=filename,
            mode="prod",
        )

    # Mantém últimos 20 arquivos salvos no redis
    redis_files.sort()
    redis_files = redis_files[-20:]

    return filename, redis_files


@task
def tratar_dados(filename: str) -> dict:
    """
    Converte coordenadas X, Y para latlon e recorta área da América Latina
    """
    print("\n>>>> Started with file: ", filename)
    grid, goes16_extent, info = main(filename)
    del grid, goes16_extent
    return info


@task
def salvar_parquet(info: dict, file_path: str) -> Union[str, Path]:
    """
    Converter dados de tif para parquet
    """
    # print('\n>>>> Started with info: ', ingoes16_extentfo)
    variable = info["variable"]
    datetime_save = info["datetime_save"]
    print(f"Saving {variable} in parquet")
    output_path = save_parquet(variable, datetime_save, file_path)
    return output_path


@task
def checa_update(arg_1, arg_2):
    return arg_1 == arg_2
