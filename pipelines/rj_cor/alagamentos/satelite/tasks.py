"""
Tasks for emd
"""

import datetime as dt
import os
from pathlib import Path
from typing import Tuple, Union

import numpy as np
import pendulum
from prefect import task
import s3fs

from pipelines.rj_cor.alagamentos.satelite.satellite_utils import main, save_parquet


@task(nout=5)
def slice_data(current_time: str) -> Tuple[str, str, str, str, str]:
    '''
    slice data em ano. mês, dia, hora e dia juliano
    '''
    if not isinstance(current_time, str):
        current_time = current_time.to_datetime_string()

    current_time = current_time or pendulum.now("utc").to_datetime_string()

    ano = current_time[:4]
    mes = current_time[5:7]
    dia = current_time[8:10]
    hora = current_time[11:13]
    dia_juliano = dt.datetime.strptime(
        current_time, '%Y-%m-%d %H:%M:%S').strftime('%j')
    return ano, mes, dia, hora, dia_juliano


@task
def download(variavel: str,
             ano: str,
             dia_juliano: str,
             hora: str) -> Union[str, Path]:
    '''
    Acessa o S3 e faz o download do primeiro arquivo da data-hora especificada
    '''
    # Use the anonymous credentials to access public data
    s3_fs = s3fs.S3FileSystem(anon=True)

    # Get first file of GOES-16 data (multiband format) at this time
    file = np.sort(np.array(s3_fs.find(
        f'noaa-goes16/ABI-L2-{variavel}/{ano}/{dia_juliano}/{hora}/')
    ))[0]

    base_path = os.path.join(
        os.getcwd(), 'data', 'satelite', variavel[:-1], 'input')

    if not os.path.exists(base_path):
        os.makedirs(base_path)
    print('>>>>>>>>>>>>>>> basepath',  base_path)

    filename = os.path.join(base_path, file.split('/')[-1])
    s3_fs.get(file, filename)
    return filename


@task
def tratar_dados(filename: str) -> dict:
    '''
    Converte coordenadas X, Y para latlon e recorta área da América Latina
    '''
    print('\n>>>> Started with file: ', filename)
    grid, goes16_extent, info = main(filename)
    del grid, goes16_extent
    return info


@task
def salvar_parquet(info: dict) -> Union[str, Path]:
    '''
    Converter dados de tif para parquet
    '''
    # print('\n>>>> Started with info: ', ingoes16_extentfo)
    variable = info['variable']
    datetime_save = info['datetime_save']
    print(f'Saving {variable} in parquet')
    filename = save_parquet(variable, datetime_save)
    return filename
