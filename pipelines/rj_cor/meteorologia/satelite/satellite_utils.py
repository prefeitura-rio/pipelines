# -*- coding: utf-8 -*-
# pylint: disable=too-many-locals
# flake8: noqa
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

import datetime
import os
from pathlib import Path
import re
from typing import Tuple, Union

from google.cloud import storage
import netCDF4 as nc
import numpy as np
from osgeo import gdal  # pylint: disable=E0401
import pandas as pd
import pendulum
import xarray as xr

from pipelines.rj_cor.meteorologia.satelite.remap import remap
from pipelines.utils.utils import get_credentials_from_env, list_blobs_with_prefix, log


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
    # storage_client = storage.Client()

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


def get_info(path: str) -> Tuple[dict, str]:
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
    if procura_m == -1:
        procura_m = path.find("-M3")
    if procura_m == -1:
        procura_m = path.find("-M4")
    product = path[path.find("L2-") + 3 : procura_m]
    # print(product)

    # Nem todos os produtos foram adicionados no dicionário de características
    # dos produtos. Olhar arquivo original caso o produto não estaja aqui
    product_caracteristics = {}
    # CMIPF - Cloud and Moisture Imagery: 'CMI'
    product_caracteristics["CMIPF"] = {
        "variable": "CMI",
        "vmin": -50,
        "vmax": 50,
        "cmap": "jet",
        "resolution": 3,
    }
    # CMIPC - Cloud and Moisture Imagery: 'CMI'
    product_caracteristics["CMIPC"] = {
        "variable": "CMI",
        "vmin": -50,
        "vmax": 50,
        "cmap": "jet",
        "resolution": 3,
    }
    # CMIPM - Cloud and Moisture Imagery: 'CMI'
    product_caracteristics["CMIPM"] = {
        "variable": "CMI",
        "vmin": -50,
        "vmax": 50,
        "cmap": "jet",
        "resolution": 1,
    }
    # ACHAF - Cloud Top Height: 'HT'
    product_caracteristics["ACHAF"] = {
        "variable": "HT",
        "vmin": 0,
        "vmax": 15000,
        "cmap": "rainbow",
        "resolution": 3,
    }
    # ACHTF - Cloud Top Temperature: 'TEMP'
    product_caracteristics["ACHATF"] = {
        "variable": "TEMP",
        "vmin": 180,
        "vmax": 300,
        "cmap": "jet",
        "resolution": 3,
    }
    # ACMF - Clear Sky Masks: 'BCM'
    product_caracteristics["ACMF"] = {
        "variable": "BCM",
        "vmin": 0,
        "vmax": 1,
        "cmap": "gray",
        "resolution": 3,
    }
    # ACTPF - Cloud Top Phase: 'Phase'
    product_caracteristics["ACTPF"] = {
        "variable": "Phase",
        "vmin": 0,
        "vmax": 5,
        "cmap": "jet",
        "resolution": 3,
    }
    # ADPF - Aerosol Detection: 'Smoke'
    product_caracteristics["ADPF"] = {
        "variable": "Smoke",
        "vmin": 0,
        "vmax": 255,
        "cmap": "jet",
        "resolution": 3,
    }
    # AODF - Aerosol Optical Depth: 'AOD'
    product_caracteristics["AODF"] = {
        "variable": "AOD",
        "vmin": 0,
        "vmax": 2,
        "cmap": "rainbow",
        "resolution": 3,
    }
    # CODF - Cloud Optical Depth: 'COD'
    product_caracteristics["CODF"] = {
        "variable": "CODF",
        "vmin": 0,
        "vmax": 100,
        "cmap": "jet",
        "resolution": 3,
    }
    # CPSF - Cloud Particle Size: 'PSD'
    product_caracteristics["CPSF"] = {
        "variable": "PSD",
        "vmin": 0,
        "vmax": 80,
        "cmap": "rainbow",
        "resolution": 3,
    }
    # CTPF - Cloud Top Pressure: 'PRES'
    product_caracteristics["CTPF"] = {
        "variable": "PRES",
        "vmin": 0,
        "vmax": 1100,
        "cmap": "rainbow",
        "resolution": 3,
    }
    # DSIF - Derived Stability Indices: 'CAPE', 'KI', 'LI', 'SI', 'TT'
    product_caracteristics["DSIF"] = {
        "variable": "CAPE",
        "vmin": 0,
        "vmax": 1000,
        "cmap": "jet",
        "resolution": 3,
    }
    # FDCF - Fire-Hot Spot Characterization: 'Area', 'Mask', 'Power', 'Temp'
    product_caracteristics["FDCF"] = {
        "variable": "Mask",
        "vmin": 0,
        "vmax": 255,
        "cmap": "jet",
        "resolution": 3,
    }
    # LSTF - Land Surface (Skin) Temperature: 'LST'
    product_caracteristics["LSTF"] = {
        "variable": "LST",
        "vmin": 213,
        "vmax": 330,
        "cmap": "jet",
        "resolution": 3,
    }
    # RRQPEF - Rainfall Rate - Quantitative Prediction Estimate: 'RRQPE'
    product_caracteristics["RRQPEF"] = {
        "variable": "RRQPE",
        "vmin": 0,
        "vmax": 50,
        "cmap": "jet",
        "resolution": 3,
    }
    # SSTF - Sea Surface (Skin) Temperature: 'SST'
    product_caracteristics["SSTF"] = {
        "variable": "SSTF",
        "vmin": 268,
        "vmax": 308,
        "cmap": "jet",
        "resolution": 3,
    }
    # TPWF - Total Precipitable Water: 'TPW'
    product_caracteristics["TPWF"] = {
        "variable": "TPW",
        "vmin": 0,
        "vmax": 60,
        "cmap": "jet",
        "resolution": 3,
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
        regex = r"-M\\dC\\d"   # noqa: W605
        find_expression = re.findall(regex, path)[0]
        product_caracteristics["band"] = int(
            (path[path.find(find_expression) + 4 : path.find("_G16")])
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
    data = year + "-" + month.zfill(2) + "-" + day.zfill(2)
    partitions = os.path.join(
        f"ano_particao={year}",
        f"mes_particao={month}",
        f"data_particao={data}",
        f"hora_particao={time_save}",
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


def save_data_in_file(
    variable: str, datetime_save: str, file_path: str
) -> Union[str, Path]:
    """
    Save data in parquet
    """
    date_save = datetime_save[:8]
    time_save = str(int(datetime_save[9:11]))

    year = date_save[:4]
    month = str(int(date_save[4:6]))
    day = str(int(date_save[6:8]))
    date = year + "-" + month.zfill(2) + "-" + day.zfill(2)
    partitions = os.path.join(
        f"ano_particao={year}",
        f"mes_particao={month}",
        f"data_particao={date}",
        f"hora_particao={time_save}",
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

    # cria pasta de partições se elas não existem
    output_path = os.path.join(os.getcwd(), "data", "satelite", variable, "output")
    parquet_path = os.path.join(output_path, partitions)

    if not os.path.exists(parquet_path):
        os.makedirs(parquet_path)

    data["horario"] = pendulum.from_format(
        datetime_save, "YYYYMMDD HHmmss"
    ).to_time_string()
    # log(f">>>>> data head {data.head()}")
    # Fixa ordem das colunas
    data = data[["longitude", "latitude", "horario", variable.lower()]]

    # salva em csv
    filename = file_path.split("/")[-1].replace(".nc", "")
    log(f"Saving {filename} on {parquet_path}")
    log(f"Data_save: {date_save}, time_save: {time_save}")
    file_path = os.path.join(parquet_path, f"{filename}.csv")
    data.to_csv(file_path, index=False)
    return output_path


def main(path: Union[str, Path]):
    """
    Função principal para converter dados x,y em lon,lat
    """
    # Create the basemap reference for the Rectangular Projection.
    # You may choose the region you want.
    # Full Disk Extent
    # extent = [-156.00, -81.30, 6.30, 81.30]
    # Brazil region
    # extent = [-90.0, -40.0, -20.0, 10.0]
    # Região da cidade do Rio de Janeiro
    # lat_max, lon_min = (-22.802842397418548, -43.81200531887697)
    # lat_min, lon_max = (-23.073487725280266, -43.11300020870994)
    # Região da cidade do Rio de Janeiro meteorologista
    lat_max, lon_max = (
        -21.699774257353113,
        -42.35676996062447,
    )  # canto superior direito
    lat_min, lon_min = (
        -23.801876626302175,
        -45.05290312102409,
    )  # canto inferior esquerdo
    # Estado do RJ
    # lat_max, lon_max = (-20.69080839963545, -40.28483671464648)
    # lat_min, lon_min = (-23.801876626302175, -45.05290312102409)
    extent = [lon_min, lat_min, lon_max, lat_max]

    # Get information from the image file
    product_caracteristics, datetime_save = get_info(path)
    # product, variable, vmin, vmax, cmap, band

    # Choose the image resolution (the higher the number the faster the processing is)
    resolution = product_caracteristics["resolution"]

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
