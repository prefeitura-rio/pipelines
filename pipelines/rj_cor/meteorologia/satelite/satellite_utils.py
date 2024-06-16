# -*- coding: utf-8 -*-
# pylint: disable=too-many-locals, R0913, R1732
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
import requests


import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
import fiona
from google.cloud import storage
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pendulum
import s3fs
import xarray as xr

from pipelines.rj_cor.meteorologia.satelite.remap import remap
from pipelines.utils.utils import (
    get_credentials_from_env,
    get_vault_secret,
    list_blobs_with_prefix,
    log,
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
    Get UTC date-hour on 'YYYYMMDD HHmm' format and returns im the same format but
    on São Paulo timezone.
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


def get_files_from_aws(partition_path):
    """
    Get all available files from aws that is inside the partition path
    """
    log("Acessing AWS to get files")
    # Use the anonymous credentials to access public data
    s3_fs = s3fs.S3FileSystem(anon=True)

    # Get all files of GOES-16 data (multiband format) at this hour
    storage_files_path = np.sort(
        np.array(
            s3_fs.find(f"noaa-goes16/{partition_path}")
            # s3_fs.find(f"noaa-goes16/ABI-L2-CMIPF/2022/270/10/OR_ABI-L2-CMIPF-M6C13_G16_s20222701010208_e20222701019528_c20222701020005.nc")
        )
    )
    storage_origin = "aws"

    return storage_files_path, storage_origin, s3_fs


def get_files_from_gcp(partition_path):
    """
    Get all available files from gcp that is inside the partition path
    """
    log("Acessing GCP to get files")
    bucket_name = "gcp-public-data-goes-16"
    storage_files_path = get_blob_with_prefix(
        bucket_name=bucket_name, prefix=partition_path, mode="prod"
    )
    storage_origin = "gcp"
    return storage_files_path, storage_origin, bucket_name


def choose_file_to_download(
    storage_files_path: list, base_path: str, redis_files: str, ref_filename=None
):
    """
    We can treat only one file each run, so we will first eliminate files that were
    already trated and saved on redis and keep only the first one from this partition
    """
    # keep only ref_filename if it exists
    if ref_filename is not None:
        # extract this part of the name s_20222911230206_e20222911239514
        ref_date = ref_filename[ref_filename.find("_s") + 1 : ref_filename.find("_e")]
        log(f"\n\n[DEBUG]: ref_date: {ref_date}")
        match_text = re.compile(f".*{ref_date}")
        storage_files_path = list(filter(match_text.match, storage_files_path))

    log(f"\n\n[DEBUG]: storage_files_path: {storage_files_path}")

    # keep the first file if it is not on redis
    storage_files_path.sort()
    destination_file_path, download_file = None, None

    for path_file in storage_files_path:
        filename = path_file.split("/")[-1]

        log(f"\n\nChecking if {filename} is in redis")
        if filename not in redis_files:
            log(f"\n\n {filename} not in redis")
            redis_files.append(filename)
            destination_file_path = os.path.join(base_path, filename)
            download_file = path_file
            # log(f"[DEBUG]: filename to be append on redis_files: {redis_files}")
            break
        log(f"\n{filename} is already in redis")

    return redis_files, destination_file_path, download_file


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
        # "vmin": 0,
        # "vmax": 1000,
        "vmin": {"LI": -10, "CAPE": 0, "TT": -43, "SI": -10, "KI": 0},
        "vmax": {"LI": 40, "CAPE": 5000, "TT": 56, "SI": 10, "KI": 40},
        # https://www.star.nesdis.noaa.gov/goesr/documents/ATBDs/Enterprise/ATBD_Enterprise_Soundings_Legacy_Atmospheric_Profiles_v3.1_2019-11-01.pdf
        # Lifted Index: --10 to 40 K
        # CAPE: 0 to 5000 J/kg
        # Showalter index: >4 to -10 K
        # Total totals Index: -43 to > 56
        # K index: 0 to 40
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
        "variable": ["SST"],
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
    # MCMIPF - Cloud and Moisture Imagery: 'MCMIP'
    # https://developers.google.com/earth-engine/datasets/catalog/NOAA_GOES_16_MCMIPM#bands

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

    log(f"Product Caracteristics: {product_caracteristics}")

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
        log(
            f"Starting remap for path: {path}, remap_path: {remap_path}, variable: {variable[i]}"
        )
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

    folder_path = f"{os.getcwd()}/temp/treated/{product}/"
    # cria pasta de partições se elas não existem
    output_path = os.path.join(os.getcwd(), "temp", "output", mode_redis, product)
    partitions_path = os.path.join(output_path, partitions)

    if not os.path.exists(partitions_path):
        os.makedirs(partitions_path)

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

    print(f"Final df: {data.head()}")
    # Fixa ordem das colunas
    data = data[["longitude", "latitude", "horario"] + [i.lower() for i in variable]]
    print("cols", data.columns)

    file_name = files[0].split("_variable-")[0]
    print(f"\n\n[DEGUB]: Saving {file_name} on {output_path}\n")
    print(f"Data_save: {date_save}, time_save: {time_save}")
    # log(f"\n\n[DEGUB]: Saving {file_name} on {parquet_path}\n\n")
    # log(f"Data_save: {date_save}, time_save: {time_save}")
    file_path = os.path.join(partitions_path, f"{file_name}.csv")
    data.to_csv(file_path, index=False)
    return output_path, file_path


def get_variable_values(dfr: pd.DataFrame, variable: str) -> xr.DataArray:
    """
    Convert pandas dataframe to a matrix with latitude on rows, longitudes on
    columns and the correspondent values on a xarray DataArray
    """

    log("Fill matrix")
    dfr = dfr.sort_values(by=["latitude", "longitude"], ascending=[False, True])
    matrix_temp = dfr.pivot(index="latitude", columns="longitude", values=variable)
    matrix_temp = matrix_temp.sort_index(ascending=False)
    log(
        f"[DEBUG]: matriz de conversão deve estar com a latitude em ordem descendente\
             e a longitude em ascendente: {matrix_temp.head(10)}"
    )

    # Create a NumPy matriz NumPy
    matrix = matrix_temp.values

    longitudes = list(matrix_temp.columns)
    latitudes = list(matrix_temp.index)

    log("Convert df to xr data array")
    data_array = xr.DataArray(
        matrix, dims=("lat", "lon"), coords={"lon": longitudes, "lat": latitudes}
    )
    log("Finished converting df to data array")

    return data_array


# pylint: disable=dangerous-default-value
def get_point_value(
    data_array: xr.DataArray, selected_point: list = [-22.89980, -43.35546]
) -> float:
    """
    Find the nearest point on data_array from the selected_point and return its value
    """

    # Find the nearest index of latitude and longitude from selected_point
    lat_idx = (data_array["lat"] - selected_point[0]).argmin().values
    lon_idx = (data_array["lon"] - selected_point[1]).argmin().values

    # Get the correspondent value of this point
    point_value = data_array.isel(lat=lat_idx, lon=lon_idx).values
    log(
        f"\nThe value of the selected point is {point_value}. It will be replace by 0 if is nan.\n"
    )
    point_value = 0 if np.isnan(point_value) else float(point_value)

    return point_value


# pylint: disable=unused-variable
def create_and_save_image(data: xr.DataArray, info: dict, variable) -> Path:
    """
    Create image from xarray ans save it as png file.
    """

    plt.figure(figsize=(10, 10))

    # Use the Geostationary projection in cartopy
    axis = plt.axes(projection=ccrs.PlateCarree())

    extent = info["extent"]
    img_extent = [extent[0], extent[2], extent[1], extent[3]]

    # Define the color scale based on the channel
    colormap = "jet"  # White to black for IR channels
    # colormap = "gray_r" # White to black for IR channels
    log(f"\nmax valueeeeeeeeeeeeeeeeee: {np.nanmax(data)} min value: {np.nanmin(data)}")

    variable = variable.upper()
    vmin = info["vmin"]
    vmax = info["vmax"]
    if variable in ["LI", "CAPE", "TT", "SI", "KI"]:
        # vmin = vmin[variable] TODO: get the correct range for each variable
        # vmax = vmax[variable]
        vmin = np.nanmin(data)
        vmax = np.nanmax(data)

    # Plot the image
    img = axis.imshow(
        data,
        origin="upper",
        extent=img_extent,
        cmap=colormap,
        alpha=0.8,
        vmin=vmin,
        vmax=vmax,
    )

    # Find shapefile file "Limite_Bairros_RJ.shp" across the entire file system
    for root, dirs, files in os.walk(os.sep):
        if "Limite_Bairros_RJ.shp" in files:
            log(f"[DEBUG] ROOT {root}")
            shapefile_dir = Path(root)
            break
    else:
        print("File not found.")

    # # Add coastlines, borders and gridlines
    # shapefile_dir = Path(
    #     "/opt/venv/lib/python3.9/site-packages/pipelines/utils/shapefiles"
    # )
    shapefile_path_neighborhood = shapefile_dir / "Limite_Bairros_RJ.shp"
    shapefile_path_state = shapefile_dir / "Limite_Estados_BR_IBGE.shp"

    log("\nImporting shapefiles")
    fiona.os.environ["SHAPE_RESTORE_SHX"] = "YES"
    reader_neighborhood = shpreader.Reader(shapefile_path_neighborhood)
    reader_state = shpreader.Reader(shapefile_path_state)
    state = [record.geometry for record in reader_state.records()]
    neighborhood = [record.geometry for record in reader_neighborhood.records()]
    log("\nShapefiles imported")
    axis.add_geometries(
        state, ccrs.PlateCarree(), facecolor="none", edgecolor="black", linewidth=0.7
    )
    axis.add_geometries(
        neighborhood,
        ccrs.PlateCarree(),
        facecolor="none",
        edgecolor="black",
        linewidth=0.2,
    )
    # axis.coastlines(resolution='10m', color='black', linewidth=1.0)
    # axis.add_feature(cartopy.feature.BORDERS, edgecolor='black', linewidth=1.0)
    grdln = axis.gridlines(
        crs=ccrs.PlateCarree(),
        color="gray",
        alpha=0.7,
        linestyle="--",
        linewidth=0.7,
        xlocs=np.arange(-180, 180, 1),
        ylocs=np.arange(-90, 90, 1),
        draw_labels=True,
    )
    grdln.top_labels = False
    grdln.right_labels = False

    plt.colorbar(
        img,
        label=variable.upper(),
        extend="both",
        orientation="horizontal",
        pad=0.05,
        fraction=0.05,
    )

    log("\n Start saving image")
    output_image_path = Path(os.getcwd()) / "output" / "images"

    save_image_path = output_image_path / (f"{variable}_{info['datetime_save']}.png")

    if not output_image_path.exists():
        output_image_path.mkdir(parents=True, exist_ok=True)

    plt.savefig(save_image_path, bbox_inches="tight", pad_inches=0.1, dpi=80)
    log(f"\n Ended saving image on {save_image_path}")
    return save_image_path


# def upload_image_to_api(info: dict, save_image_path: Path):
def upload_image_to_api(var: str, save_image_path: Path, point_value: float):
    """
    Upload image and point value to API.
    """
    # We need to change this variable so it can be posted on API
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
