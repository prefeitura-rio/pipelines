# -*- coding: utf-8 -*-
# pylint: disable=unsubscriptable-object
"""
Converte coordenada X,Y para latlon
"""
import math
import re
import netCDF4 as nc
import numpy as np
from osgeo import osr, gdal  # pylint: disable=E0401
from pipelines.utils.utils import log


def extract_resolution(input_string: str):
    """
    Extract resolution from file and convert it from rad to degrees
    """
    # Define the regular expression pattern
    pattern = r"y:\s*([0-9.]+)\s*rad\s*x:\s*([0-9.]+)\s*rad"

    # Use re.search to find the pattern in the input string
    match = re.search(pattern, input_string)

    # Check if the pattern was found
    if match:
        # Extract y and x values
        y_value = math.degrees(float(match.group(1)))
        x_value = math.degrees(float(match.group(2)))

        # Print the extracted values
        print(f"y value: {y_value}, x value: {x_value}")
    else:
        print("Pattern not found in the input string.")
    return y_value, x_value


def export_image(image, path):
    """
    Export imagem em tif
    """
    driver = gdal.GetDriverByName("netCDF")
    return driver.CreateCopy(path, image, 0)


def get_scale_offset(path, variable):
    """
    Obtem o scale offset
    """
    data = nc.Dataset(path, mode="r")

    if variable in ("BCM", "Phase", "Smoke", "Dust", "Mask", "Power"):
        scale = 1
        offset = 0
    else:
        scale = data.variables[variable].scale_factor
        offset = data.variables[variable].add_offset
    # scale = 0
    # offset = 0
    data.close()
    return scale, offset


def remap(
    path: str, remap_path: str, variable: list, extent: list
):  # pylint: disable=too-many-locals
    """
    Converte coordenada X, Y para latlon
    """
    # Open the file
    log(f"Remaping NETCDF:{path}:{variable}")
    img = gdal.Open(f"NETCDF:{path}:" + variable)

    # Read the header metadata
    metadata = img.GetMetadata()
    scale = float(metadata.get(variable + "#scale_factor"))
    offset = float(metadata.get(variable + "#add_offset"))
    # units = metadata.get(variable + '#units')
    dqf_variable = metadata.get(variable + "#ancillary_variables").split(" ")[0]
    resolution = metadata.get(variable + "#resolution")
    undef = (
        float(metadata.get(variable + "#_FillValue"))
        if metadata.get(variable + "#_FillValue") != ""
        else np.nan
    )

    print(f"scale/offset/undef/resolution: {scale}/{offset}/{undef}/{resolution}")
    print(variable.lower())

    # Open dqf file
    dqf = gdal.Open(f"NETCDF:{path}:{dqf_variable}")  # adicionado

    # Load the data
    data = img.ReadAsArray(0, 0, img.RasterXSize, img.RasterYSize).astype(float)
    data_dqf = dqf.ReadAsArray(0, 0, dqf.RasterXSize, dqf.RasterYSize).astype(
        float
    )  # adicionado

    # Remove undef
    data[data == undef] = np.nan  # adicionado

    # Apply the scale, offset and convert to celsius if necessary
    data = data * scale + offset

    # Apply NaN's where the quality flag is greater than 1
    data[data_dqf > 0] = np.nan  # adicionado

    # Read the original file projection and configure the output projection
    source_prj = osr.SpatialReference()
    source_prj.ImportFromProj4(img.GetProjectionRef())

    target_prj = osr.SpatialReference()
    target_prj.ImportFromProj4("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")

    # Reproject the data
    geot = img.GetGeoTransform()
    driver = gdal.GetDriverByName("MEM")
    raw = driver.Create("raw", data.shape[0], data.shape[1], 1, gdal.GDT_Float32)
    raw.SetGeoTransform(geot)
    raw.GetRasterBand(1).WriteArray(data)

    y_res, x_res = extract_resolution(resolution)

    # Define the parameters of the output file
    options = gdal.WarpOptions(
        format="netCDF",
        srcSRS=source_prj,
        dstSRS=target_prj,
        outputBounds=(extent[0], extent[3], extent[2], extent[1]),
        outputBoundsSRS=target_prj,
        outputType=gdal.GDT_Float32,
        srcNodata=undef,
        dstNodata="nan",
        xRes=x_res,
        yRes=y_res,
        multithread=True,
        resampleAlg=gdal.GRA_NearestNeighbour,
        copyMetadata=True,
    )

    # Write the reprojected file on disk
    remap_filename = (
        f"{path.split('/')[-1].split('.nc')[0]}_variable-{variable.lower()}.nc"
    )

    gdal.Warp(remap_path + remap_filename, raw, options=options)
    print(f"\nRemap saved as {remap_filename} on {remap_path}")
