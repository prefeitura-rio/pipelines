# -*- coding: utf-8 -*-
# pylint: disable=W0102, W0613, R0913, R0914, R0915
"""
Tasks for emd
"""

import datetime as dt
import os
import re
from pathlib import Path
from typing import Union

import requests

import pandas as pd
import pendulum
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped

from pipelines.rj_cor.meteorologia.satelite.satellite_utils import (
    create_and_save_image,
    choose_file_to_download,
    download_blob,
    extract_julian_day_and_hour_from_filename,
    get_files_from_aws,
    get_files_from_gcp,
    get_info,
    get_point_value,
    get_variable_values,
    remap_g16,
    save_data_in_file,
    # upload_image_to_api,
)
from pipelines.utils.utils import log, get_vault_secret


@task()
def get_dates(current_time, product) -> str:
    """
    Task para obter o dia atual caso nenhuma data tenha sido passada
    Subtraimos 5 minutos da hora atual pois o último arquivo que sobre na aws
    sempre cai na hora seguinte (Exemplo: o arquivo
    OR_ABI-L2-RRQPEF-M6_G16_s20230010850208_e20230010859516_c20230010900065.nc
    cujo início da medição foi às 08:50 foi salvo na AWS às 09:00:33).
    """
    if current_time is None:
        current_time = pendulum.now("UTC").subtract(minutes=5).to_datetime_string()
    # Product sst is updating one hour later
    if product == "SSTF":
        current_time = pendulum.now("UTC").subtract(minutes=55).to_datetime_string()
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
    product: str,
    date_hour_info: dict,
    band: str = None,
    ref_filename: str = None,
    redis_files: list = [],
    wait=None,
    mode_redis: str = "prod",
) -> Union[str, Path]:
    """
    Access S3 or GCP and download the first file on this specified date hour
    that is not already saved on redis
    """

    year = date_hour_info["year"]
    julian_day = date_hour_info["julian_day"]
    hour_utc = date_hour_info["hour_utc"][:2]
    partition_path = f"ABI-L2-{product}/{year}/{julian_day}/{hour_utc}/"
    log(f"Getting files from {partition_path}")

    storage_files_path, storage_origin, storage_conection = get_files_from_aws(
        partition_path
    )
    log(storage_files_path)
    if len(storage_files_path) == 0:
        storage_files_path, storage_origin, storage_conection = get_files_from_gcp(
            partition_path
        )

    # Keep only files from specified band
    if product == "CMIPF":
        # para capturar banda 13
        storage_files_path = [
            f for f in storage_files_path if bool(re.search("C" + band, f))
        ]

    # Skip task if there is no file on API
    if len(storage_files_path) == 0:
        log("No available files on API")
        skip = Skipped("No available files on API")
        raise ENDRUN(state=skip)

    base_path = os.path.join(os.getcwd(), "temp", "input", mode_redis, product[:-1])

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    # Seleciona primeiro arquivo que não tem o nome salvo no redis
    log(f"\n\n[DEBUG]: available files on API: {storage_files_path}")
    log(f"\n\n[DEBUG]: filenames that are already saved on redis_files: {redis_files}")

    redis_files, destination_file_path, download_file = choose_file_to_download(
        storage_files_path, base_path, redis_files, ref_filename
    )

    # Skip task if there is no new file
    if download_file is None:
        log("No new available files")
        skip = Skipped("No new available files")
        raise ENDRUN(state=skip)

    # Download file from aws or gcp
    if storage_origin == "aws":
        storage_conection.get(download_file, destination_file_path)
    else:
        download_blob(
            bucket_name=storage_conection,
            source_blob_name=download_file,
            destination_file_name=destination_file_path,
            mode="prod",
        )

    return destination_file_path, redis_files


@task
def tratar_dados(filename: str) -> dict:
    """
    Convert X, Y coordinates from netcdf file to a latlon coordinates
    and select only the specified region on extent variable.
    """
    log(f"\n Started treating file: {filename}")
    # Create the basemap reference for the Rectangular Projection.
    # You may choose the region you want.

    # Full Disk Extent
    # extent = [-156.00, -81.30, 6.30, 81.30]

    # Brazil region
    # extent = [-90.0, -40.0, -20.0, 10.0]

    # Estado do RJ
    # lat_max, lon_max = (-20.69080839963545, -40.28483671464648)
    # lat_min, lon_min = (-23.801876626302175, -45.05290312102409)

    # Região da cidade do Rio de Janeiro
    # lat_max, lon_min = (-22.802842397418548, -43.81200531887697)
    # lat_min, lon_max = (-23.073487725280266, -43.11300020870994)

    # Recorte da região da cidade do Rio de Janeiro segundo meteorologista
    lat_max, lon_max = (
        -21.699774257353113,
        -42.35676996062447,
    )  # canto superior direito
    lat_min, lon_min = (
        -23.801876626302175,
        -45.05290312102409,
    )  # canto inferior esquerdo

    extent = [lon_min, lat_min, lon_max, lat_max]

    # Get informations from the nc file
    product_caracteristics = get_info(filename)
    product_caracteristics["extent"] = extent

    # Call the remap function to convert x, y to lon, lat and save converted file
    remap_g16(
        filename,
        extent,
        product=product_caracteristics["product"],
        variable=product_caracteristics["variable"],
    )

    return product_caracteristics


@task(nout=2)
def save_data(info: dict, mode_redis: str = "prod") -> Union[str, Path]:
    """
    Concat all netcdf data and save partitioned by date on a csv
    """

    log("Start saving product on a csv")
    output_path, output_filepath = save_data_in_file(
        product=info["product"],
        variable=info["variable"],
        datetime_save=info["datetime_save"],
        mode_redis=mode_redis,
    )
    return output_path, output_filepath


@task
def create_image_and_upload_to_api(info: dict, output_filepath: Path):
    """
    Create image from dataframe, get the value of a point on the image and send these to API.
    """

    dfr = pd.read_csv(output_filepath)

    dfr = dfr.sort_values(by=["latitude", "longitude"], ascending=[False, True])

    for var in info["variable"]:
        log(f"\nStart creating image for variable {var}\n")

        var = var.lower()
        data_array = get_variable_values(dfr, var)
        point_value = get_point_value(data_array)

        # Get the pixel values
        data = data_array.data[:]
        log(f"\n[DEBUG] {var} data \n{data}")
        log(f"\nmax value: {data.max()} min value: {data.min()}")
        save_image_path = create_and_save_image(data, info, var)

        log(f"\nStart uploading image for variable {var} on API\n")
        # upload_image_to_api(var, save_image_path, point_value)
        var = "cp" if var == "cape" else var

        log("Getting API url")
        url_secret = get_vault_secret("rionowcast")["data"]
        log(f"urlsecret1 {url_secret}")
        url_secret = url_secret["url_api_satellite_products"]
        log(f"urlsecret2 {url_secret}")
        api_url = f"{url_secret}/{var.lower()}"
        log(
            f"\n Sending image {save_image_path} to API: {api_url} with value {point_value}\n"
        )

        payload = {"value": point_value}

        # Convert from Path to string
        save_image_path = str(save_image_path)

        with open(save_image_path, "rb") as image_file:
            files = {"image": (save_image_path, image_file, "image/jpeg")}
            response = requests.post(api_url, data=payload, files=files)

        if response.status_code == 200:
            log("Finished the request successful!")
            log(response.json())
        else:
            log(f"Error: {response.status_code}, {response.text}")
        log(save_image_path)
        log(f"\nEnd uploading image for variable {var} on API\n")
