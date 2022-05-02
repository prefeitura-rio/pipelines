# -*- coding: utf-8 -*-
# pylint: disable=too-many-locals
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
# along with this program. If not, see http://www.gnu.org/licenses/.
####################################################################

# ===================================================================
# Acronym Description
# ===================================================================
# ACHAF - Cloud Top Height: 'HT'
# ACHTF - Cloud Top Temperature: 'TEMP'
# ACMF - Clear Sky Masks: 'BCM'
# ACTPF - Cloud Top Phase: 'Phase'
# ADPF - Aerosol Detection: 'Smoke'
# ADPF - Aerosol Detection: 'Dust'
# AODF - Aerosol Optical Depth: 'AOD'
# CMIPF - Cloud and Moisture Imagery: 'CMI'
# CMIPC - Cloud and Moisture Imagery: 'CMI'
# CMIPM - Cloud and Moisture Imagery: 'CMI'
# CODF - Cloud Optical Depth: 'COD'
# CPSF - Cloud Particle Size: 'PSD'
# CTPF - Cloud Top Pressure: 'PRES'
# DMWF - Derived Motion Winds: 'pressure'
# DMWF - Derived Motion Winds: 'temperature'
# DMWF - Derived Motion Winds: 'wind_direction'
# DMWF - Derived Motion Winds: 'wind_speed'
# DSIF - Derived Stability Indices: 'CAPE'
# DSIF - Derived Stability Indices: 'KI'
# DSIF - Derived Stability Indices: 'LI'
# DSIF - Derived Stability Indices: 'SI'
# DSIF - Derived Stability Indices: 'TT'
# DSRF - Downward Shortwave Radiation: 'DSR'
# FDCF - Fire-Hot Spot Characterization: 'Area'
# FDCF - Fire-Hot Spot Characterization: 'Mask'
# FDCF - Fire-Hot Spot Characterization: 'Power'
# FDCF - Fire-Hot Spot Characterization: 'Temp'
# FSCF - Snow Cover: 'FSC'
# LSTF - Land Surface (Skin) Temperature: 'LST'
# RRQPEF - Rainfall Rate - Quantitative Prediction Estimate: 'RRQPE'
# RSR - Reflected Shortwave Radiation: 'RSR'
# SSTF - Sea Surface (Skin) Temperature: 'SST'
# TPWF - Total Precipitable Water: 'TPW'
# VAAF - Volcanic Ash: 'VAH'
# VAAF - Volcanic Ash: 'VAML'

# ====================================================================
# Required Libraries
# ====================================================================

import base64
import datetime
import json
import os
from pathlib import Path
from typing import Tuple, Union

from google.oauth2 import service_account
from google.cloud import storage
import netCDF4 as nc
import numpy as np
from osgeo import gdal  # pylint: disable=E0401
import pandas as pd
import pendulum
import xarray as xr

from pipelines.rj_cor.meteorologia.satelite.remap import remap
from pipelines.utils.utils import log


def get_credentials_from_env(mode: str = "prod") -> service_account.Credentials:
    """
    Gets credentials from env vars
    """
    if mode not in ["prod", "staging"]:
        raise ValueError("Mode must be 'prod' or 'staging'")
    env: str = os.getenv(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()}", "")
    if env == "":
        raise ValueError(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()} env var not set!")
    info: dict = json.loads(base64.b64decode(env))

    return service_account.Credentials.from_service_account_info(info)


def list_blobs_with_prefix(bucket_name: str, prefix: str, mode: str = "prod") -> str:
    """
    Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    Mode needs to be "prod" or "staging"
    """

    credentials = get_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    files = []

    for blob in blobs:
        files.append(blob.name)

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
    datahora = pendulum.from_format(datetime_save, "YYYYMMDD HHmm")
    datahora = datahora.in_tz("America/Sao_Paulo")
    return datahora.format("YYYYMMDD HHmm")


def get_info(path: str) -> Tuple[dict, str]:
    """
    # Getting Information From the File Name  (Time, Date,
    # Product Type, Variable and Defining the CMAP)
    """
    # Search for the Scan start in the file name
    start = path[path.find("_s") + 2 : path.find("_e")]
    # Converting from julian day to dd-mm-yyyy
    year = int(start[0:4])
    # Subtract 1 because the year starts at "0"
    dayjulian = int(start[4:7]) - 1
    # Convert from julian to conventional
    dayconventional = datetime.datetime(year, 1, 1) + datetime.timedelta(dayjulian)
    # Format the date according to the strftime directives
    # date = dayconventional.strftime('%d-%b-%Y')
    # Time of the start of the Scan
    # time = start[7:9] + ":" + start[9:11] + ":" + start[11:13] + " UTC"

    # Date as string
    date_save = dayconventional.strftime("%Y%m%d")

    # Time (UTC) as string
    time_save = start[7:11]
    datetime_save = str(date_save) + " " + time_save

    # Converte data/hora de UTC para horário de São Paulo
    datetime_save = converte_timezone(datetime_save=datetime_save)

    # =====================================================================
    # Detect the product type
    # =====================================================================
    procura_m = path.find("-M6")
    if procura_m == -1:
        procura_m = path.find("-M3")
    if procura_m == -1:
        procura_m = path.find("-M4")
    product = path[path.find("L2-") + 3 : procura_m]
    print(product)

    # Nem todos os produtos foram adicionados no dicionário de características
    # dos produtos. Olhar arquivo original caso o produto não estaja aqui
    product_caracteristics = {}
    # CMIPF - Cloud and Moisture Imagery: 'CMI'
    product_caracteristics["CMIPF"] = {
        "variable": "CMI",
        "vmin": -50,
        "vmax": 50,
        "cmap": "jet",
    }
    # CMIPC - Cloud and Moisture Imagery: 'CMI'
    product_caracteristics["CMIPC"] = {
        "variable": "CMI",
        "vmin": -50,
        "vmax": 50,
        "cmap": "jet",
    }
    # CMIPM - Cloud and Moisture Imagery: 'CMI'
    product_caracteristics["CMIPM"] = {
        "variable": "CMI",
        "vmin": -50,
        "vmax": 50,
        "cmap": "jet",
    }
    # ACHAF - Cloud Top Height: 'HT'
    product_caracteristics["ACHAF"] = {
        "variable": "HT",
        "vmin": 0,
        "vmax": 15000,
        "cmap": "rainbow",
    }
    # ACHTF - Cloud Top Temperature: 'TEMP'
    product_caracteristics["ACHATF"] = {
        "variable": "TEMP",
        "vmin": 180,
        "vmax": 300,
        "cmap": "jet",
    }
    # ACMF - Clear Sky Masks: 'BCM'
    product_caracteristics["ACMF"] = {
        "variable": "BCM",
        "vmin": 0,
        "vmax": 1,
        "cmap": "gray",
    }
    # ACTPF - Cloud Top Phase: 'Phase'
    product_caracteristics["ACTPF"] = {
        "variable": "Phase",
        "vmin": 0,
        "vmax": 5,
        "cmap": "jet",
    }
    # ADPF - Aerosol Detection: 'Smoke'
    product_caracteristics["ADPF"] = {
        "variable": "Smoke",
        "vmin": 0,
        "vmax": 255,
        "cmap": "jet",
    }
    # AODF - Aerosol Optical Depth: 'AOD'
    product_caracteristics["AODF"] = {
        "variable": "AOD",
        "vmin": 0,
        "vmax": 2,
        "cmap": "rainbow",
    }
    # CODF - Cloud Optical Depth: 'COD'
    product_caracteristics["CODF"] = {
        "variable": "CODF",
        "vmin": 0,
        "vmax": 100,
        "cmap": "jet",
    }
    # CPSF - Cloud Particle Size: 'PSD'
    product_caracteristics["CPSF"] = {
        "variable": "PSD",
        "vmin": 0,
        "vmax": 80,
        "cmap": "rainbow",
    }
    # CTPF - Cloud Top Pressure: 'PRES'
    product_caracteristics["CTPF"] = {
        "variable": "PRES",
        "vmin": 0,
        "vmax": 1100,
        "cmap": "rainbow",
    }
    # DSIF - Derived Stability Indices: 'CAPE', 'KI', 'LI', 'SI', 'TT'
    product_caracteristics["DSIF"] = {
        "variable": "CAPE",
        "vmin": 0,
        "vmax": 1000,
        "cmap": "jet",
    }
    # FDCF - Fire-Hot Spot Characterization: 'Area', 'Mask', 'Power', 'Temp'
    product_caracteristics["FDCF"] = {
        "variable": "Mask",
        "vmin": 0,
        "vmax": 255,
        "cmap": "jet",
    }
    # LSTF - Land Surface (Skin) Temperature: 'LST'
    product_caracteristics["LSTF"] = {
        "variable": "LST",
        "vmin": 213,
        "vmax": 330,
        "cmap": "jet",
    }
    # RRQPEF - Rainfall Rate - Quantitative Prediction Estimate: 'RRQPE'
    product_caracteristics["RRQPEF"] = {
        "variable": "RRQPE",
        "vmin": 0,
        "vmax": 50,
        "cmap": "jet",
    }
    # SSTF - Sea Surface (Skin) Temperature: 'SST'
    product_caracteristics["SSTF"] = {
        "variable": "SSTF",
        "vmin": 268,
        "vmax": 308,
        "cmap": "jet",
    }
    # TPWF - Total Precipitable Water: 'TPW'
    product_caracteristics["TPWF"] = {
        "variable": "TPW",
        "vmin": 0,
        "vmax": 60,
        "cmap": "jet",
    }

    # variable = product_caracteristics[product]['variable']
    # vmin = product_caracteristics[product]['vmin']
    # vmax = product_caracteristics[product]['vmax']
    # cmap = product_caracteristics[product]['cmap']
    product_caracteristics = product_caracteristics[product]
    product_caracteristics["product"] = product
    variable = product_caracteristics["variable"]

    if variable == "CMI":
        # Search for the GOES-16 channel in the file name
        product_caracteristics["band"] = int(
            (path[path.find("M3C" or "M4C") + 3 : path.find("_G16")])
        )
    else:
        product_caracteristics["band"] = np.nan

    return product_caracteristics, datetime_save


def get_goes_extent(data: pd.DataFrame) -> list:
    """
    define espatial limits
    """
    pph = data.variables["goes_imager_projection"].perspective_point_height
    x_1 = data.variables["x_image_bounds"][0] * pph
    x_2 = data.variables["x_image_bounds"][1] * pph
    y_1 = data.variables["y_image_bounds"][1] * pph
    y_2 = data.variables["y_image_bounds"][0] * pph
    goes16_extent = [x_1, y_1, x_2, y_2]

    # Get the latitude and longitude image bounds
    # geo_extent = data.variables['geospatial_lat_lon_extent']
    # min_lon = float(geo_extent.geospatial_westbound_longitude)
    # max_lon = float(geo_extent.geospatial_eastbound_longitude)
    # min_lat = float(geo_extent.geospatial_southbound_latitude)
    # max_lat = float(geo_extent.geospatial_northbound_latitude)
    return goes16_extent


def remap_g16(
    path: Union[str, Path],
    extent: list,
    resolution: int,
    variable: str,
    datetime_save: str,
):
    """
    the GOES-16 image is reprojected to the rectangular projection in the extent region
    """
    # Open the file using the NetCDF4 library
    data = nc.Dataset(path)
    # see_data = np.ma.getdata(data.variables[variable][:])
    # print('\n\n>>>>>> netcdf ', np.unique(see_data)[:100])

    # Calculate the image extent required for the reprojection
    goes16_extent = get_goes_extent(data)

    # Close the NetCDF file after getting the data
    data.close()

    # Call the reprojection funcion
    grid = remap(path, variable, extent, resolution, goes16_extent)

    #     You may export the grid to GeoTIFF (and any other format supported by GDAL).
    # using GDAL from osgeo
    time_save = str(int(datetime_save[9:11]))

    year = datetime_save[:4]
    month = str(int(datetime_save[4:6]))
    day = str(int(datetime_save[6:8]))
    partitions = os.path.join(
        f"ano={year}", f"mes={month}", f"dia={day}", f"hora={time_save}"
    )

    tif_path = os.path.join(
        os.getcwd(), "data", "satelite", variable, "temp", partitions
    )

    if not os.path.exists(tif_path):
        os.makedirs(tif_path)

    # Export the result to GeoTIFF
    driver = gdal.GetDriverByName("GTiff")
    filename = os.path.join(tif_path, "dados.tif")
    driver.CreateCopy(filename, grid, 0)
    return grid, goes16_extent


def treat_data(
    data: pd.DataFrame, variable: str, reprojection_variables: dict
) -> pd.DataFrame:
    """
    Treat nans and Temperature data, extent, product, variable, date_save,
    time_save, bmap, cmap, vmin, vmax, dpi, band, path
    """
    if variable in (
        "Dust",
        "Smoke",
        "TPW",
        "PRES",
        "HT",
        "TEMP",
        "AOD",
        "COD",
        "PSD",
        "CAPE",
        "KI",
        "LI",
        "SI",
        "TT",
        "FSC",
        "RRQPE",
        "VAML",
        "VAH",
    ):
        data[data == max(data[0])] = np.nan
        data[data == min(data[0])] = np.nan

    if variable == "SST":
        data[data == max(data[0])] = np.nan
        data[data == min(data[0])] = np.nan

        # Call the reprojection funcion again to get only the valid SST pixels
        path = reprojection_variables["path"]
        extent = reprojection_variables["extent"]
        resolution = reprojection_variables["resolution"]
        goes16_extent = reprojection_variables["goes16_extent"]

        grid = remap(path, "DQF", extent, resolution, goes16_extent)
        data_dqf = grid.ReadAsArray()
        # If the Quality Flag is not 0, set as NaN
        data[data_dqf != 0] = np.nan

    if variable == "Mask":
        data[data == -99] = np.nan
        data[data == 40] = np.nan
        data[data == 50] = np.nan
        data[data == 60] = np.nan
        data[data == 150] = np.nan
        data[data == max(data[0])] = np.nan
        data[data == min(data[0])] = np.nan

    if variable == "BCM":
        data[data == 255] = np.nan
        data[data == 0] = np.nan

    if variable == "Phase":
        data[data >= 5] = np.nan
        data[data == 0] = np.nan

    if variable == "LST":
        data[data >= 335] = np.nan
        data[data <= 200] = np.nan

    return data


def save_parquet(variable: str, datetime_save: str) -> Union[str, Path]:
    """
    Save data in parquet
    """
    date_save = datetime_save[:8]
    time_save = str(int(datetime_save[9:11]))

    year = date_save[:4]
    month = str(int(date_save[4:6]))
    day = str(int(date_save[-2:]))
    partitions = os.path.join(
        f"ano={year}", f"mes={month}", f"dia={day}", f"hora={time_save}"
    )

    tif_data = os.path.join(
        os.getcwd(), "data", "satelite", variable, "temp", partitions, "dados.tif"
    )

    data = xr.open_dataset(tif_data, engine="rasterio")
    # print('>>>>>>>>>>>>>>> data', data['band_data'].values)

    # Converte para dataframe trocando o nome das colunas

    data = (
        data.to_dataframe()
        .reset_index()[["x", "y", "band_data"]]
        .rename(
            {
                "y": "latitude",
                "x": "longitude",
                "band_data": f"{variable.lower()}",
            },
            axis=1,
        )
    )

    # cria pasta se ela não existe
    base_path = os.path.join(os.getcwd(), "data", "satelite", variable, "output")

    parquet_path = os.path.join(base_path, partitions)

    if not os.path.exists(parquet_path):
        os.makedirs(parquet_path)

    # Fixa ordem das colunas
    print(">>>>>", ["longitude", "latitude", variable.lower()])
    data = data[["longitude", "latitude", variable.lower()]]

    # salva em parquet
    log(f"Saving on base_path {base_path}")
    filename = os.path.join(parquet_path, "dados.csv")
    data.to_csv(filename, index=False)
    # filename = os.path.join(parquet_path, 'dados.parquet')
    # data.to_parquet(filename, index=False)
    return base_path


def main(path: Union[str, Path]):
    """
    Função principal para converter dados x,y em lon,lat
    """
    # Create the basemap reference for the Rectangular Projection.
    # You may choose the region you want.
    # Full Disk Extent
    # extent = [-156.00, -81.30, 6.30, 81.30]
    # Brazil region
    extent = [-90.0, -40.0, -20.0, 10.0]
    # # Região da cidade do Rio de Janeiro
    # lat_max, lon_min = (-22.802842397418548, -43.81200531887697)
    # lat_min, lon_max = (-23.073487725280266, -43.11300020870994)
    # extent = [lon_min, lat_min, lon_max, lat_max]

    # Choose the image resolution (the higher the number the faster the processing is)
    resolution = 5

    # Get information from the image file
    product_caracteristics, datetime_save = get_info(path)
    # product, variable, vmin, vmax, cmap, band

    # Call the remap function to convert x, y to lon, lat and save geotiff
    grid, goes16_extent = remap_g16(
        path,
        extent,
        resolution,
        product_caracteristics["variable"],
        datetime_save,
    )

    info = {
        "product": product_caracteristics["product"],
        "variable": product_caracteristics["variable"],
        "vmin": product_caracteristics["vmin"],
        "vmax": product_caracteristics["vmax"],
        "cmap": product_caracteristics["cmap"],
        "datetime_save": datetime_save,
        "band": product_caracteristics["band"],
        "extent": extent,
        "resolution": resolution,
    }

    return grid, goes16_extent, info
