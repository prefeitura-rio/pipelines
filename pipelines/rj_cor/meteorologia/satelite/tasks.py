# -*- coding: utf-8 -*-
# pylint: disable=W0102, W0613, R0913, R0914
"""
Tasks for emd
"""

import datetime as dt
import os
import re
from pathlib import Path
from typing import Union

import numpy as np
import pendulum
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped
import s3fs

from pipelines.rj_cor.meteorologia.satelite.satellite_utils import (
    extract_julian_day_and_hour_from_filename,
    main,
    save_data_in_file,
    get_blob_with_prefix,
    download_blob,
)
from pipelines.utils.utils import log


@task()
def get_dates(current_time) -> str:
    """
    Task para obter o dia atual caso nenhuma data tenha sido passada
    Subtraimos 5 minutos da hora atual pois o último arquivo que sobre na aws
    sempre cai na hora seguinte (Exemplo: o arquivo
    OR_ABI-L2-RRQPEF-M6_G16_s20230010850208_e20230010859516_c20230010900065.nc
    cujo início da medição foi às 08:50 foi salvo na AWS às 09:00:33).
    """
    if current_time is None:
        current_time = pendulum.now("UTC").subtract(minutes=5).to_datetime_string()
    return current_time


@task(nout=1)
def slice_data(current_time: str, ref_filename: str = None) -> dict:
    """
    slice data to separate in year, julian_day, month, day and hour in UTC
    """
    if ref_filename is not None:
        year, julian_day, hour_utc = extract_julian_day_and_hour_from_filename(
            ref_filename
        )
        month = None
        day = None
    else:
        year = current_time[:4]
        month = current_time[5:7]
        day = current_time[8:10]
        hour_utc = current_time[11:13]
        julian_day = dt.datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S").strftime(
            "%j"
        )

    date_hour_info = {
        "year": str(year),
        "julian_day": str(julian_day),
        "month": str(month),
        "day": str(day),
        "hour_utc": str(hour_utc),
    }

    return date_hour_info


@task(nout=2, max_retries=10, retry_delay=dt.timedelta(seconds=60))
def download(
    variavel: str,
    date_hour_info: dict,
    band: str = None,
    ref_filename: str = None,
    redis_files: list = [],
    wait=None,
    mode_redis: str = "prod",
) -> Union[str, Path]:
    """
    Acessa o S3 e faz o download do primeiro arquivo da data-hora especificada
    """

    year = date_hour_info["year"]
    julian_day = date_hour_info["julian_day"]
    hour_utc = date_hour_info["hour_utc"][:2]

    try:
        # Use the anonymous credentials to access public data
        s3_fs = s3fs.S3FileSystem(anon=True)

        # Get all files of GOES-16 data (multiband format) at this hour
        path_files = np.sort(
            np.array(
                s3_fs.find(
                    f"noaa-goes16/ABI-L2-{variavel}/{year}/{julian_day}/{hour_utc}/"
                )
                # s3_fs.find(f"noaa-goes16/ABI-L2-CMIPF/2022/270/10/OR_ABI-L2-CMIPF-M6C13_G16_s20222701010208_e20222701019528_c20222701020005.nc")
            )
        )
        # Mantém apenas arquivos de determinada banda
        if variavel == "CMIPF":
            # para capturar banda 13
            path_files = [f for f in path_files if bool(re.search("C" + band, f))]
        origem = "aws"
    except IndexError:
        bucket_name = "gcp-public-data-goes-16"
        partition_file = f"ABI-L2-{variavel}/{year}/{julian_day}/{hour_utc}/"
        path_files = get_blob_with_prefix(
            bucket_name=bucket_name, prefix=partition_file, mode="prod"
        )
        origem = "gcp"

    # Skip task if there is no file on API
    if len(path_files) == 0:
        log("No available files on API")
        skip = Skipped("No available files on API")
        raise ENDRUN(state=skip)

    base_path = os.path.join(
        os.getcwd(), mode_redis, "data", "satelite", variavel[:-1], "input"
    )

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    # Seleciona primeiro arquivo que não tem o nome salvo no redis
    log(f"\n\n[DEBUG]: available files on API: {path_files}")
    log(f"\n\n[DEBUG]: filenames already saved on redis_files: {redis_files}")
    log(f"\n\n[DEBUG]: ref_filename: {ref_filename}")

    # keep only ref_filename if it exists
    if ref_filename is not None:
        # extract this part of the name s_20222911230206_e20222911239514
        ref_date = ref_filename[ref_filename.find("_s") + 1 : ref_filename.find("_e")]
        log(f"\n\n[DEBUG]: ref_date: {ref_date}")
        match_text = re.compile(f".*{ref_date}")
        path_files = list(filter(match_text.match, path_files))
        log(f"\n\n[DEBUG]: path_files: {path_files}")

    # keep the first file if it is not on redis
    path_files.sort()
    download_file = None
    for path_file in path_files:
        filename = path_file.split("/")[-1]
        log(f"\n\n[DEBUG]: {filename} check if is in redis")
        if filename not in redis_files:
            log(f"\n\n[DEBUG]: {filename} not in redis")
            redis_files.append(filename)
            path_filename = os.path.join(base_path, filename)
            download_file = path_file
            # log(f"[DEBUG]: filename to be append on redis_files: {redis_files}")
            break

    # Skip task if there is no new file
    if download_file is None:
        log("No new available files")
        skip = Skipped("No new available files")
        raise ENDRUN(state=skip)

    # Faz download da aws ou da gcp
    if origem == "aws":
        s3_fs.get(download_file, path_filename)
    else:
        download_blob(
            bucket_name=bucket_name,
            source_blob_name=download_file,
            destination_file_name=path_filename,
            mode="prod",
        )

    return path_filename, redis_files


@task
def tratar_dados(filename: str, mode_redis: str = "prod") -> dict:
    """
    Converte coordenadas X, Y para latlon e recorta área
    """
    log(f"\n>>>> Started treating file: {filename}")
    grid, goes16_extent, info = main(filename, mode_redis)
    del grid, goes16_extent
    return info


@task
def save_data(info: dict, file_path: str, mode_redis: str = "prod") -> Union[str, Path]:
    """
    Convert tif data to csv
    """

    variable = info["variable"]
    datetime_save = info["datetime_save"]
    print(f"Saving {variable} in parquet")
    output_path = save_data_in_file(variable, datetime_save, file_path, mode_redis)
    return output_path
