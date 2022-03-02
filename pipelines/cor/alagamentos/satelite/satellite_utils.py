'''
Funções úteis no tratamento de dados de satélite
'''
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
# import pip._internal as pip #import pip
#import sys
from typing import Tuple

import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import netCDF4 as nc
import numpy as np
from osgeo import gdal   # pylint: disable=E0401
import xarray as xr

from pipelines.cor.alagamentos.satelite.cpt_convert import load_cpt
from pipelines.cor.alagamentos.satelite.remap import remap

# try:
#     from osgeo import osr, gdal  # pylint: disable=E0401
# except ImportError:
#     # Confere se o python é o 3.8
#     if sys.version[:3] == '3.8':
#         package = os.path.join(os.getcwd(),
#                       'pacotes_gdal',
#                       'GDAL-3.4.1-cp38-cp38-manylinux_2_5_x86_64.manylinux1_x86_64.whl')
#         pip.main(['install', package])
#     else:
#         print('Problema ao instalar gdal no python ', sys.version[:3])
#     from osgeo import osr, gdal  # pylint: disable=E0401

# cpt_convert explanation
# https://geonetcast.wordpress.com/2017/06/02/geonetclass-manipulating-goes-16-data-with-python-part-v/


def get_info(path: str) -> Tuple[dict, str]:
    '''
    # Getting Information From the File Name  (Time, Date,
    # Product Type, Variable and Defining the CMAP)
    '''
    # Search for the Scan start in the file name
    start = (path[path.find("_s")+2:path.find("_e")])
    # Converting from julian day to dd-mm-yyyy
    year = int(start[0:4])
    # Subtract 1 because the year starts at "0"
    dayjulian = int(start[4:7]) - 1
    # Convert from julian to conventional
    dayconventional = datetime.datetime(
        year, 1, 1) + datetime.timedelta(dayjulian)
    # Format the date according to the strftime directives
    # date = dayconventional.strftime('%d-%b-%Y')
    # Time of the start of the Scan
    # time = start[7:9] + ":" + start[9:11] + ":" + start[11:13] + " UTC"
    # Date as string
    date_save = dayconventional.strftime('%Y%m%d')
    # Time (UTC) as string
    time_save = start[7:9] + start[9:11]
    datetime_save = str(date_save) + ' ' + time_save

    # =====================================================================
    # Detect the product type
    # =====================================================================
    product = (path[path.find("L2-")+3:path.find("-M6")])  # "-M3" or "-M4"
    print(product)

    # Nem todos os produtos foram adicionados no dicionário de características
    # dos produtos. Olhar arquivo original caso o produto não estaja aqui
    product_caracteristics = {}
    # CMIPF - Cloud and Moisture Imagery: 'CMI'
    product_caracteristics['CMIPF'] = {'variable': 'CMI',
                                       'vmin': -50,
                                       'vmax': 50,
                                       'cmap': "jet"}
    # CMIPC - Cloud and Moisture Imagery: 'CMI'
    product_caracteristics['CMIPC'] = {'variable': 'CMI',
                                       'vmin': -50,
                                       'vmax': 50,
                                       'cmap': "jet"}
    # CMIPM - Cloud and Moisture Imagery: 'CMI'
    product_caracteristics['CMIPM'] = {'variable': 'CMI',
                                       'vmin': -50,
                                       'vmax': 50,
                                       'cmap': "jet"}
    # ACHAF - Cloud Top Height: 'HT'
    product_caracteristics['ACHAF'] = {'variable': 'HT',
                                       'vmin': 0,
                                       'vmax': 15000,
                                       'cmap': "rainbow"}
    # ACHTF - Cloud Top Temperature: 'TEMP'
    product_caracteristics['ACHATF'] = {'variable': 'TEMP',
                                        'vmin': 180,
                                        'vmax': 300,
                                        'cmap': "jet"}
    # ACMF - Clear Sky Masks: 'BCM'
    product_caracteristics['ACMF'] = {'variable': 'BCM',
                                      'vmin': 0,
                                      'vmax': 1,
                                      'cmap': "gray"}
    # ACTPF - Cloud Top Phase: 'Phase'
    product_caracteristics['ACTPF'] = {'variable': 'Phase',
                                       'vmin': 0,
                                       'vmax': 5,
                                       'cmap': "jet"}
    # ADPF - Aerosol Detection: 'Smoke'
    product_caracteristics['ADPF'] = {'variable': 'Smoke',
                                      'vmin': 0,
                                      'vmax': 255,
                                      'cmap': "jet"}
    # AODF - Aerosol Optical Depth: 'AOD'
    product_caracteristics['AODF'] = {'variable': 'AOD',
                                      'vmin': 0,
                                      'vmax': 2,
                                      'cmap': "rainbow"}
    # CODF - Cloud Optical Depth: 'COD'
    product_caracteristics['CODF'] = {'variable': 'CODF',
                                      'vmin': 0,
                                      'vmax': 100,
                                      'cmap': "jet"}
    # CPSF - Cloud Particle Size: 'PSD'
    product_caracteristics['CPSF'] = {'variable': 'PSD',
                                      'vmin': 0,
                                      'vmax': 80,
                                      'cmap': "rainbow"}
    # CTPF - Cloud Top Pressure: 'PRES'
    product_caracteristics['CTPF'] = {'variable': 'PRES',
                                      'vmin': 0,
                                      'vmax': 1100,
                                      'cmap': "rainbow"}
    # DSIF - Derived Stability Indices: 'CAPE', 'KI', 'LI', 'SI', 'TT'
    product_caracteristics['DSIF'] = {'variable': 'CAPE',
                                      'vmin': 0,
                                      'vmax': 1000,
                                      'cmap': "jet"}
    # FDCF - Fire-Hot Spot Characterization: 'Area', 'Mask', 'Power', 'Temp'
    product_caracteristics['FDCF'] = {'variable': 'Mask',
                                      'vmin': 0,
                                      'vmax': 255,
                                      'cmap': "jet"}
    # LSTF - Land Surface (Skin) Temperature: 'LST'
    product_caracteristics['LSTF'] = {'variable': 'LST',
                                      'vmin': 213,
                                      'vmax': 330,
                                      'cmap': "jet"}
    # RRQPEF - Rainfall Rate - Quantitative Prediction Estimate: 'RRQPE'
    product_caracteristics['RRQPEF'] = {'variable': 'RRQPE',
                                        'vmin': 0,
                                        'vmax': 50,
                                        'cmap': "jet"}
    # SSTF - Sea Surface (Skin) Temperature: 'SST'
    product_caracteristics['SSTF'] = {'variable': 'SSTF',
                                      'vmin': 268,
                                      'vmax': 308,
                                      'cmap': "jet"}
    # TPWF - Total Precipitable Water: 'TPW'
    product_caracteristics['TPWF'] = {'variable': 'TPW',
                                      'vmin': 0,
                                      'vmax': 60,
                                      'cmap': "jet"}

    #variable = product_caracteristics[product]['variable']
    #vmin = product_caracteristics[product]['vmin']
    #vmax = product_caracteristics[product]['vmax']
    #cmap = product_caracteristics[product]['cmap']
    product_caracteristics = product_caracteristics[product]
    product_caracteristics['product'] = product
    variable = product_caracteristics['variable']

    if variable == "CMI":
        # Search for the GOES-16 channel in the file name
        product_caracteristics['band'] = int(
            (path[path.find("M3C" or "M4C")+3:path.find("_G16")]))
    else:
        product_caracteristics['band'] = np.nan

    return product_caracteristics, datetime_save


def get_goes_extent(data):
    '''
    define espatial limits
    '''
    pph = data.variables['goes_imager_projection'].perspective_point_height
    x_1 = data.variables['x_image_bounds'][0] * pph
    x_2 = data.variables['x_image_bounds'][1] * pph
    y_1 = data.variables['y_image_bounds'][1] * pph
    y_2 = data.variables['y_image_bounds'][0] * pph
    goes16_extent = [x_1, y_1, x_2, y_2]

    # Get the latitude and longitude image bounds
    # geo_extent = data.variables['geospatial_lat_lon_extent']
    # min_lon = float(geo_extent.geospatial_westbound_longitude)
    # max_lon = float(geo_extent.geospatial_eastbound_longitude)
    # min_lat = float(geo_extent.geospatial_southbound_latitude)
    # max_lat = float(geo_extent.geospatial_northbound_latitude)
    return goes16_extent


def remap_g16(path, extent, resolution, variable, datetime_save):
    '''
    the GOES-16 image is reprojected to the rectangular projection in the extent region
    '''
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

    tif_path = os.path.join(os.getcwd(), 'data', 'satelite', variable, 'temp',
                            f'ano={year}', f'mes={month}',
                            f'dia={day}', f'hora={time_save}')
    if not os.path.exists(tif_path):
        os.makedirs(tif_path)

    # Export the result to GeoTIFF
    driver = gdal.GetDriverByName('GTiff')
    filename = os.path.join(tif_path, 'dados.tif')
    driver.CreateCopy(filename, grid, 0)
    return grid, goes16_extent


def treat_data(data, variable, reprojection_variables):
    '''
    Treat nans and Temperature data, extent, product, variable, date_save,
    time_save, bmap, cmap, vmin, vmax, dpi, band, path
    '''
    if variable in ('Dust', 'Smoke', 'TPW', 'PRES', 'HT', 'TEMP', 'AOD',
                    'COD', 'PSD', 'CAPE', 'KI', 'LI', 'SI', 'TT', 'FSC',
                    'RRQPE', 'VAML', 'VAH'):
        data[data == max(data[0])] = np.nan
        data[data == min(data[0])] = np.nan

    if variable == "SST":
        data[data == max(data[0])] = np.nan
        data[data == min(data[0])] = np.nan

        # Call the reprojection funcion again to get only the valid SST pixels
        path = reprojection_variables['path']
        extent = reprojection_variables['extent']
        resolution = reprojection_variables['resolution']
        goes16_extent = reprojection_variables['goes16_extent']

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


def plot_g16(data, product_caracteristics, datetime_save,
             bmap, dpi):
    '''
    Plota produto em latlon e salva imagem
    '''
    variable = product_caracteristics['variable']
    cmap = product_caracteristics['cmap']
    vmin = product_caracteristics['vmin']
    vmax = product_caracteristics['vmax']
    band = product_caracteristics['band']

    date_save = datetime_save[:8]
    time_save = datetime_save[9:]

    # Create a GOES-16 bands string array
    # Wavelenghts = ['[]','[0.47 μm]','[0.64 μm]','[0.865 μm]','[1.378 μm]','[1.61 μm]',
    #                '[2.25 μm]','[3.90 μm]','[6.19 μm]','[6.95 μm]','[7.34 μm]','[8.50 μm]',
    #                '[9.61 μm]', '[10.35 μm]','[11.20 μm]','[12.30 μm]','[13.30 μm]']

    # Choose a title for the plot
    # Title = " GOES-16 ABI CMI band " + str(band) + "       " +  Wavelenghts[int(band)] +\
    #  "       " + unit + "       " + date + "       " + time
    # Insert the institution name
    #Institution = "GNC-A Blog"

    if variable == "CMI":
        if band <= 6:
            # Converts a CPT file to be used in Python
            cpt = load_cpt(
                'E:\\VLAB\\Python\\Colortables\\Square Root Visible Enhancement.cpt')
            # Makes a linear interpolation
            cpt_convert = LinearSegmentedColormap('cpt', cpt)
            # Plot the GOES-16 channel with the converted CPT colors (you may alter the min and
            # max to match your preference)
            bmap.imshow(data, origin='upper', cmap=cpt_convert, vmin=0, vmax=1)
            # Insert the colorbar at the bottom
        elif band == 7:
            # Converts a CPT file to be used in Python
            cpt = load_cpt('E:\\VLAB\\Python\\Colortables\\SVGAIR2_TEMP.cpt')
            # Makes a linear interpolation
            cpt_convert = LinearSegmentedColormap('cpt', cpt)
            # Plot the GOES-16 channel with the converted CPT colors (you may alter the min
            # and max to match your preference)
            bmap.imshow(data, origin='upper', cmap=cpt_convert,
                        vmin=-112.15, vmax=56.85)
            # Insert the colorbar at the bottom
        elif 7 < band < 11:
            # Converts a CPT file to be used in Python
            cpt = load_cpt('E:\\VLAB\\Python\\Colortables\\SVGAWVX_TEMP.cpt')
            # Makes a linear interpolation
            cpt_convert = LinearSegmentedColormap('cpt', cpt)
            # Plot the GOES-16 channel with the converted CPT colors (you may alter the
            # min and max to match your preference)
            bmap.imshow(data, origin='upper', cmap=cpt_convert,
                        vmin=-112.15, vmax=56.85)
            # Insert the colorbar at the bottom
        elif band > 10:
            # Converts a CPT file to be used in Python
            cpt = load_cpt('E:\\VLAB\\Python\\Colortables\\IR4AVHRR6.cpt')
            # Makes a linear interpolation
            cpt_convert = LinearSegmentedColormap('cpt', cpt)
            # Plot the GOES-16 channel with the converted CPT colors (you may alter
            # the min and max to match your preference)
            #bmap.imshow(data, origin='upper', cmap='gray', vmin=-103, vmax=84)
            bmap.imshow(data, origin='upper', cmap='gray_r', vmin=-70, vmax=40)
            # Insert the colorbar at the bottom
    else:
        bmap.imshow(data, origin='upper', cmap=cmap, vmin=vmin, vmax=vmax)

    #cb = bmap.colorbar(location='bottom', size = '1%', pad = '-1.0%')
    # cb.outline.set_visible(False)
    # Remove the colorbar outline
    #cb.ax.tick_params(width = 0)
    # Remove the colorbar ticks
    # Put the colobar labels inside the colorbar
    # cb.ax.xaxis.set_tick_params(pad=-17)
    # Change the color and size of the colorbar labels
    #cb.ax.tick_params(axis='x', colors='black', labelsize=12)

    # Save the result
    fig_name = variable + '_' + date_save + time_save + '.png'
    print('fig name',  fig_name)
    plt.savefig(fig_name, dpi=dpi, pad_inches=0)
    # plt.savefig('C:\VLAB\GOES-16_Ch13.png', dpi=dpi, bbox_inches='tight', pad_inches=0)


def save_parquet(variable, datetime_save):
    '''
    Save data in parquet
    '''
    date_save = datetime_save[:8]
    time_save = str(int(datetime_save[9:11]))

    year = date_save[:4]
    month = str(int(date_save[4:6]))
    day = str(int(date_save[-2:]))
    tif_data = os.path.join(os.getcwd(), 'data', 'satelite', variable, 'temp',
                            f'ano={year}', f'mes={month}',
                            f'dia={day}', f'hora={time_save}', 'dados.tif')

    data = xr.open_dataset(tif_data, engine='rasterio')
    # print('>>>>>>>>>>>>>>> data', data['band_data'].values)

    # Converte para dataframe trocando o nome das colunas

    data = data.to_dataframe().reset_index()[['x', 'y', 'band_data']]\
        .rename({'y': 'latitude', 'x': 'longitude', 'band_data': f'{variable.lower()}'}, axis=1)

    # cria pasta se ela não existe
    parquet_path = os.path.join(os.getcwd(), 'data', 'satelite', variable, 'output',
                                f'ano={year}', f'mes={month}',
                                f'dia={day}', f'hora={time_save}')

    if not os.path.exists(parquet_path):
        os.makedirs(parquet_path)

    # salva em parquet
    print('Saving on ', parquet_path)
    filename = os.path.join(parquet_path, 'dados.csv')
    data.to_csv(filename, index=False)
    # filename = os.path.join(parquet_path, 'dados.parquet')
    # data.to_parquet(filename, index=False)
    return filename


def main(path):
    '''
    Função principal para converter dados x,y em lon,lat
    '''
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
    product_caracteristics,  datetime_save = get_info(path)
    # product, variable, vmin, vmax, cmap, band

    # Call the remap function to convert x, y to lon, lat and save geotiff
    grid, goes16_extent = remap_g16(path, extent, resolution,
                                    product_caracteristics['variable'],
                                    datetime_save)

    info = {'product': product_caracteristics['product'],
            'variable': product_caracteristics['variable'],
            'vmin': product_caracteristics['vmin'],
            'vmax': product_caracteristics['vmax'],
            'cmap': product_caracteristics['cmap'],
            'datetime_save': datetime_save,
            'band': product_caracteristics['band'],
            'extent': extent,
            'resolution': resolution}

    return grid, goes16_extent, info
