# -*- coding: utf-8 -*-
# pylint: disable=too-many-locals, R0913
"""
Funções úteis no tratamento de dados de satélite
"""
####################################################################
# LICENSE
# Copyright (C) 2018 - INPE - NATIONAL INSTITUTE FOR SPACE RESEARCH
# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program. If not, see http://www.gnu.org/licenses/..
####################################################################

# ===================================================================
# Acronym Description
# ===================================================================
# ACHAF - Cloud Top Height: 'HT'
# ACHTF - Cloud Top Temperature: 'TEMP'
# ACMF - Clear Sky Masks: 'BCM'
# ACTPF - Cloud Top Phase: 'Phase'
# ADPF - Aerosol Detection: 'Smoke', 'Dust'
# AODF - Aerosol Optical Depth: 'AOD'
# CMIPF - Cloud and Moisture Imagery: 'CMI'
# CODF - Cloud Optical Depth: 'COD'
# CPSF - Cloud Particle Size: 'PSD'
# CTPF - Cloud Top Pressure: 'PRES'
# DMWF - Derived Motion Winds: 'pressure', 'temperature', 'wind_direction', 'wind_speed'
# DSIF - Derived Stability Indices: 'CAPE', 'KI', 'LI', 'SI', 'TT'
# DSRF - Downward Shortwave Radiation: 'DSR'
# FDCF - Fire-Hot Spot Characterization: 'Area', 'Mask', 'Power', 'Temp'
# FSCF - Snow Cover: 'FSC'
# LSTF - Land Surface (Skin) Temperature: 'LST'
# RRQPEF - Rainfall Rate - Quantitative Prediction Estimate: 'RRQPE'
# RSR - Reflected Shortwave Radiation: 'RSR'
# SSTF - Sea Surface (Skin) Temperature: 'SST'
# TPWF - Total Precipitable Water: 'TPW'
# VAAF - Volcanic Ash: 'VAH', 'VAML'

# ====================================================================
# Required Libraries
# ====================================================================

import datetime
import os
import shutil
from pathlib import Path
import re
from typing import Union

from google.cloud import storage
import numpy as np
import pandas as pd
import pendulum
import xarray as xr

from pipelines.rj_cor.meteorologia.satelite.remap import remap
from pipelines.utils.utils import (
    get_credentials_from_env,
    list_blobs_with_prefix,
    log,
    parse_date_columns,
    to_partitions,
)


def get_blob_with_prefix(bucket_name: str, prefix: str, mode: str = "prod") -> str:
    """
    Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    Mode needs to be "prod" or "staging"
    """
    files = [b.name for b in list_blobs_with_prefix(bucket_name, prefix, mode)]
    files.sort()
    return files[0]


def download_blob(
    bucket_name: str,
    source_blob_name: str,
    destination_file_name: Union[str, Path],
    mode: str = "prod",
):
    """
    Downloads a blob from the bucket.
    Mode needs to be "prod" or "staging"

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # source_blob_name = "storage-object-name"

    # The path to which the file should be downloaded
    # destination_file_name = "local/path/to/file"
    """

    credentials = get_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    log(
        f"Downloaded storage object {source_blob_name} from bucket\
        {bucket_name} to local file {destination_file_name}."
    )


def converte_timezone(datetime_save: str) -> str:
    """
    Recebe o formato de data hora em 'YYYYMMDD HHmm' no UTC e
    retorna no mesmo formato no horário São Paulo
    """
    log(f">>>>>>> datetime_save {datetime_save}")
    datahora = pendulum.from_format(datetime_save, "YYYYMMDD HHmmss")
    log(f">>>>>>> datahora {datahora}")
    datahora = datahora.in_tz("America/Sao_Paulo")
    return datahora.format("YYYYMMDD HHmmss")


def extract_julian_day_and_hour_from_filename(filename: str):
    """
    Extract julian day and hour from satelite filename

    Parameters
    filename (str): 'OR_ABI-L2-TPWF-M6_G16_s20222901900203_e20222901909511_c20222901911473.nc'

    Returns
    year (int): 2022 (20222901900203)
    julian_day (int): 290 (20222901900203)
    hour_utc (str): 1900 (20222901900203)
    """
    # Search for the Scan start in the file name
    start = filename[filename.find("_s") + 2 : filename.find("_e")]
    # Get year
    year = int(start[0:4])
    # Get julian day
    julian_day = int(start[4:7])

    # Time (UTC) as string
    hour_utc = start[7:13]

    # Time of the start of the Scan
    # time = start[7:9] + ":" + start[9:11] + ":" + start[11:13] + " UTC"

    return year, julian_day, hour_utc


def convert_julian_to_conventional_day(year: int, julian_day: int):
    """Convert julian day to conventional

    Parameters
    year (int): 2022 (20222901900203)
    julian_day (int): 290 (20222901900203)

    Returns
    date_save (str): 19 (20222901900203)"""

    # Subtracting 1 because the year starts at day "0"
    julian_day = julian_day - 1
    dayconventional = datetime.datetime(year, 1, 1) + datetime.timedelta(julian_day)

    # Format the date according to the strftime directives
    date_save = dayconventional.strftime("%Y%m%d")

    return date_save


def get_info(path: str) -> dict:
    """
    # Getting Information From the File Name  (Time, Date,
    # Product Type, Variable and Defining the CMAP)
    """
    year, julian_day, hour_utc = extract_julian_day_and_hour_from_filename(path)

    date_save = convert_julian_to_conventional_day(year, julian_day)
    log(f">>>>>>>>>date_save {date_save}")
    log(f">>>>>>>>> hour_utc {hour_utc}")
    log(f">>>>>>>>>julian_day  {julian_day}")
    datetime_save = str(date_save) + " " + str(hour_utc)

    # Converte data/hora de UTC para horário de São Paulo
    datetime_save = converte_timezone(datetime_save=datetime_save)

    # =====================================================================
    # Detect the product type
    # =====================================================================
    procura_m = path.find("-M6")
    # Se não encontra o termo "M6" tenta encontrar "M3" e depois "M4"
    if procura_m == -1:
        procura_m = path.find("-M3")
    if procura_m == -1:
        procura_m = path.find("-M4")
    product = path[path.find("L2-") + 3 : procura_m]

    # Nem todos os produtos foram adicionados no dicionário de características
    # dos produtos. Olhar arquivo original caso o produto não estaja aqui
    # https://www.dropbox.com/s/yfopijdrplq5sjr/GNC-A%20Blog%20-%20GOES-R-Level-2-Products%20-%20Superimpose.py?dl=0
    # GNC-A Blog - GOES-R-Level-2-Products - Superimpose
    # https://geonetcast.wordpress.com/2018/06/28/goes-r-level-2-products-a-python-script/
    # https://www.dropbox.com/s/2zylbwfjfkx9a7i/GNC-A%20Blog%20-%20GOES-R-Level-2-Products.py?dl=0
    product_caracteristics = {}

    # ACHAF - Cloud Top Height: 'HT'
    product_caracteristics["ACHAF"] = {
        "variable": ["HT"],
        "vmin": 0,
        "vmax": 15000,
        "cmap": "rainbow",
    }
    # CMIPF - Cloud and Moisture Imagery: 'CMI'
    product_caracteristics["CMIPF"] = {
        "variable": ["CMI"],
        "vmin": -50,
        "vmax": 50,
        "cmap": "jet",
    }
    # ACHAF - Cloud Top Height: 'HT'
    product_caracteristics["ACHAF"] = {
        "variable": ["HT"],
        "vmin": 0,
        "vmax": 15000,
        "cmap": "rainbow",
    }
    # ACHTF - Cloud Top Temperature: 'TEMP'
    product_caracteristics["ACHATF"] = {
        "variable": ["TEMP"],
        "vmin": 180,
        "vmax": 300,
        "cmap": "jet",
    }
    # ACMF - Clear Sky Masks: 'BCM'
    product_caracteristics["ACMF"] = {
        "variable": ["BCM"],
        "vmin": 0,
        "vmax": 1,
        "cmap": "gray",
    }
    # ACTPF - Cloud Top Phase: 'Phase'
    product_caracteristics["ACTPF"] = {
        "variable": ["Phase"],
        "vmin": 0,
        "vmax": 5,
        "cmap": "jet",
    }
    # ADPF - Aerosol Detection: 'Smoke'
    product_caracteristics["ADPF"] = {
        "variable": ["Smoke"],
        "vmin": 0,
        "vmax": 255,
        "cmap": "jet",
    }
    # AODF - Aerosol Optical Depth: 'AOD'
    product_caracteristics["AODF"] = {
        "variable": ["AOD"],
        "vmin": 0,
        "vmax": 2,
        "cmap": "rainbow",
    }
    # CODF - Cloud Optical Depth: 'COD'
    product_caracteristics["CODF"] = {
        "variable": ["CODF"],
        "vmin": 0,
        "vmax": 100,
        "cmap": "jet",
    }
    # CPSF - Cloud Particle Size: 'PSD'
    product_caracteristics["CPSF"] = {
        "variable": ["PSD"],
        "vmin": 0,
        "vmax": 80,
        "cmap": "rainbow",
    }
    # CTPF - Cloud Top Pressure: 'PRES'
    product_caracteristics["CTPF"] = {
        "variable": ["PRES"],
        "vmin": 0,
        "vmax": 1100,
        "cmap": "rainbow",
    }
    # DSIF - Derived Stability Indices: 'CAPE', 'KI', 'LI', 'SI', 'TT'
    product_caracteristics["DSIF"] = {
        "variable": ["LI", "CAPE", "TT", "SI", "KI"],
        "vmin": 0,
        "vmax": 1000,
        "cmap": "jet",
    }
    # FDCF - Fire-Hot Spot Characterization: 'Area', 'Mask', 'Power', 'Temp'
    product_caracteristics["FDCF"] = {
        "variable": ["Area", "Mask", "Power", "Temp"],
        "vmin": 0,
        "vmax": 255,
        "cmap": "jet",
    }
    # LSTF - Land Surface (Skin) Temperature: 'LST'
    product_caracteristics["LSTF"] = {
        "variable": ["LST"],
        "vmin": 213,
        "vmax": 330,
        "cmap": "jet",
    }
    # RRQPEF - Rainfall Rate - Quantitative Prediction Estimate: 'RRQPE'
    product_caracteristics["RRQPEF"] = {
        "variable": ["RRQPE"],
        "vmin": 0,
        "vmax": 50,
        "cmap": "jet",
    }
    # SSTF - Sea Surface (Skin) Temperature: 'SST'
    product_caracteristics["SSTF"] = {
        "variable": ["SSTF"],
        "vmin": 268,
        "vmax": 308,
        "cmap": "jet",
    }
    # TPWF - Total Precipitable Water: 'TPW'
    product_caracteristics["TPWF"] = {
        "variable": ["TPW"],
        "vmin": 0,
        "vmax": 60,
        "cmap": "jet",
    }
    # TPWF - Total Precipitable Water: 'TPW'
    product_caracteristics["MCMIPF"] = {
        "variable": [
            "CMI_C01",
            "CMI_C02",
            "CMI_C03",
            "CMI_C04",
            "CMI_C05",
            "CMI_C06",
            "CMI_C07",
            "CMI_C08",
            "CMI_C09",
            "CMI_C10",
            "CMI_C11",
            "CMI_C12",
            "CMI_C13",
            "CMI_C14",
            "CMI_C15",
            "CMI_C16",
        ],
    }

    # variable = product_caracteristics[product]['variable']
    # vmin = product_caracteristics[product]['vmin']
    # vmax = product_caracteristics[product]['vmax']
    # cmap = product_caracteristics[product]['cmap']
    product_caracteristics = product_caracteristics[product]
    product_caracteristics["product"] = product
    product_caracteristics["filename"] = path
    product_caracteristics["datetime_save"] = datetime_save

    if product_caracteristics["variable"] == ["CMI"]:
        # Search for the GOES-16 channel in the file name
        pattern = r"M(\d+)C(\d+)_G16"
        match = re.search(pattern, path)
        print(match)
        print(match.group(1))
        product_caracteristics["band"] = match.group(2)
    else:
        product_caracteristics["band"] = np.nan

    print(f"Product Caracteristics: {product_caracteristics}")

    return product_caracteristics


def remap_g16(
    path: str,
    extent: list,
    product: str,
    variable: list,
):
    """
    the GOES-16 image is reprojected to the rectangular projection in the extent region.
    If netcdf file has more than one variable remap function will save each variable in
    a different netcdf file inside temp/ folder.
    """

    n_variables = len(variable)
    print(variable, n_variables)
    remap_path = f"{os.getcwd()}/temp/treated/{product}/"

    # This removing old files step is important to do backfill
    if os.path.exists(remap_path):
        print("Removing old files")
        shutil.rmtree(remap_path)

    os.makedirs(remap_path)
    for i in range(n_variables):
        remap(path, remap_path, variable[i], extent)


def read_netcdf(file_path: str) -> pd.DataFrame:
    """
    Function to extract data from NetCDF file and convert it to pandas DataFrame
    with the name of columns the same as the variable saved on the filename
    """
    dxr = xr.open_dataset(file_path)

    pattern = r"variable-(.*?)\.nc"

    match = re.search(pattern, file_path)

    if match:
        # Extract the content between "variable-" and ".nc"
        variable = match.group(1)

        dfr = (
            dxr.to_dataframe()
            .reset_index()[["lat", "lon", "Band1"]]
            .rename(
                {
                    "lat": "latitude",
                    "lon": "longitude",
                    "Band1": f"{variable}",
                },
                axis=1,
            )
        )

    return dfr


def save_data_in_file(
    product: str, variable: list, datetime_save: str, mode_redis: str = "prod"
):
    """
    Read all nc or tif files and save them in a unique file inside a partition
    """

    folder_path = f"{os.getcwd()}/temp/treated/{product}/"
    # cria pasta de partições se elas não existem
    output_path = os.path.join(os.getcwd(), "temp", "output", mode_redis, product)

    # Loop through all NetCDF files in the folder
    data = pd.DataFrame()
    files = [i for i in os.listdir(folder_path) if i.endswith(".nc")]
    for i, file_name in enumerate(files):
        saved_file_path = os.path.join(folder_path, file_name)
        data_temp = read_netcdf(saved_file_path)
        if i == 0:
            data = data_temp.copy()
        else:
            data = data.merge(data_temp, on=["latitude", "longitude"], how="outer")

    # Guarda horário do arquivo na coluna
    data["horario"] = pendulum.from_format(
        datetime_save, "YYYYMMDD HHmmss"
    ).to_time_string()
    data["data_medicao"] = pendulum.from_format(
        datetime_save, "YYYYMMDD HHmmss"
    ).to_date_string()

    # Fixa ordem das colunas
    data = data[
        ["longitude", "latitude", "data_medicao", "horario"]
        + [i.lower() for i in variable]
    ]
    print(f"Final df: {data.head()}")

    file_name = files[0].split("_variable-")[0]
    print(f"\n\n[DEGUB]: Saving {file_name} on {output_path}\n\n")

    partition_column = "data_medicao"
    data, partitions = parse_date_columns(data, partition_column)
    print(f"\n\n[DEGUB]: Partitions {partitions}\n\n")
    print(f"Final df: {data.head()}")

    to_partitions(
        data=data,
        partition_columns=partitions,
        savepath=output_path,
        data_type="parquet",
    )
    return output_path
