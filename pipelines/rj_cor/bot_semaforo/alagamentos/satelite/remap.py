'''
Converte coordenada X,Y para latlon
'''
import time as t

import netCDF4 as nc
import numpy as np
from osgeo import osr, gdal  # pylint: disable=E0401

# Define KM_PER_DEGREE
KM_PER_DEGREE = 111.32

# GOES-16 Spatial Reference System
sourcePrj = osr.SpatialReference()
# sourcePrj.ImportFromProj4('+proj=geos +h=35786023.0 +a=6378137.0\
# +b=6356752.31414 +f=0.00335281068119356027489803406172 +lat_0=0.0\
# +lon_0=-75 +sweep=x +no_defs')
sourcePrj.ImportFromProj4(
    '+proj=geos +h=35786000 +a=6378140 +b=6356750 +lon_0=-75 +sweep=x')

# Lat/lon WSG84 Spatial Reference System
targetPrj = osr.SpatialReference()
#targetPrj.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
targetPrj.ImportFromProj4('+proj=latlong +datum=WGS84')


def export_image(image, path):
    '''
    Export imagem em tif
    '''
    driver = gdal.GetDriverByName('netCDF')
    return driver.CreateCopy(path, image, 0)


def get_geot(extent, nlines, ncols):
    '''
    Compute resolution based on data dimension
    '''
    resx = (extent[2] - extent[0]) / ncols
    resy = (extent[3] - extent[1]) / nlines
    return [extent[0], resx, 0, extent[3], 0, -resy]


def get_scale_offset(path, variable):
    '''
    Obtem o scale offset
    '''
    data = nc.Dataset(path, mode='r')

    if variable in ("BCM", "Phase", "Smoke", "Dust", "Mask", "Power"):
        scale = 1
        offset = 0
    else:
        scale = data.variables[variable].scale_factor  # pylint: disable=unsubscriptable-object
        offset = data.variables[variable].add_offset  # pylint: disable=unsubscriptable-object
    #scale = 0
    #offset = 0
    data.close()
    return scale, offset


def remap(path, variable, extent, resolution, goes16_extent):
    '''
    Converte coordenada X, Y para latlon
    '''
    scale = 1
    offset = 0

    # GOES-16 Extent (satellite projection) [llx, lly, urx, ury]
    #goes16_extent = [x_1, y_1, x_2, y_2]

    # Setup NetCDF driver
    gdal.SetConfigOption('GDAL_NETCDF_BOTTOMUP', 'NO')

    if not variable == "DQF":
        # Read scale/offset from file
        scale, offset = get_scale_offset(path, variable)

    connection_info = 'NETCDF:\"' + path + '\"://' + variable

    raw = gdal.Open(connection_info)

    # Setup projection and geo-transformation
    raw.SetProjection(sourcePrj.ExportToWkt())
    #raw.SetGeoTransform(get_geot(goes16_extent, raw.RasterYSize, raw.RasterXSize))
    raw.SetGeoTransform(
        get_geot(goes16_extent, raw.RasterYSize, raw.RasterXSize))

    #print (KM_PER_DEGREE)
    # Compute grid dimension
    sizex = int(((extent[2] - extent[0]) * KM_PER_DEGREE) / resolution)
    sizey = int(((extent[3] - extent[1]) * KM_PER_DEGREE) / resolution)

    # Get memory driver
    mem_driver = gdal.GetDriverByName('MEM')

    # Create grid
    grid = mem_driver.Create('grid', sizex, sizey, 1, gdal.GDT_Float32)

    # Setup projection and geo-transformation
    grid.SetProjection(targetPrj.ExportToWkt())
    grid.SetGeoTransform(get_geot(extent, grid.RasterYSize, grid.RasterXSize))

    # Perform the projection/resampling

    print('Remapping', path)

    start = t.time()

    gdal.ReprojectImage(raw, grid, sourcePrj.ExportToWkt(), targetPrj.ExportToWkt(),
                        gdal.GRA_NearestNeighbour, options=['NUM_THREADS=ALL_CPUS'])

    print('- finished! Time:', t.time() - start, 'seconds')

    # Close file
    raw = None

    # Read grid data
    array = grid.ReadAsArray()

    # Mask fill values (i.e. invalid values)
    np.ma.masked_where(array, array == -1, False)

    array = array.astype(np.uint16)

    # Apply scale and offset
    array = array * scale + offset
    #np.ma.masked_where(array, array == 100, False)

    # grid.GetRasterBand(1).SetNoDataValue(-1)
    grid.GetRasterBand(1).WriteArray(array)

    return grid
