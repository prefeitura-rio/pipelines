# -*- coding: utf-8 -*-
"""
Tasks to dump data from a SICOP FTP to BigQuery
"""
# pylint: disable=E0702,E1137,E1136,E1101,C0207,W0613
from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.utils.ftp.client import FTPClient
from pipelines.utils.utils import (
    log,
    get_storage_blobs,
    to_partitions,
    parse_date_columns,
    parser_blobs_to_partition_dict,
    get_vault_secret,
)


@task
def get_ftp_client(wait=None):
    """
    Get FTP client
    """

    siscob_secret = get_vault_secret("sicop")
    hostname = siscob_secret["data"]["hostname"]
    username = siscob_secret["data"]["username"]
    password = siscob_secret["data"]["password"]

    return FTPClient(
        hostname=hostname,
        username=username,
        password=password,
    )


@task(nout=2)
def get_files_to_download(client, pattern, dataset_id, table_id, date_format):
    """
    Get files to download FTP and GCS
    """

    client.connect()
    files = client.list_files(path=".", pattern=pattern)
    log(f"files: {files}")

    blobs = get_storage_blobs(dataset_id, table_id)
    storage_partitions_dict = parser_blobs_to_partition_dict(blobs)
    storage_pattern_files = [
        date.replace("-", "") for date in storage_partitions_dict["data_particao"]
    ]

    files_to_download = files
    for partition_pattern in storage_pattern_files:
        for file in files:
            if partition_pattern == file.split("_")[1]:
                files_to_download.remove(file)

    log(f"files to download: {files_to_download}")

    download_data = files_to_download != []

    return files_to_download, download_data


@task
def download_files(client, files, save_path):
    """
    Download files from FTP
    """

    save_path = Path(save_path)
    save_path.mkdir(parents=True, exist_ok=True)

    client.connect()
    files_to_parse = []
    for file in files:
        file_path = save_path / file
        if not file_path.exists():
            client.download(remote_path=file, local_path=file_path)
            log(
                f"downloaded: {file_path}",
            )
        else:
            log(
                f"already exists: {file_path}",
            )
        files_to_parse.append(file_path)
    log(f"files_to_parse: {files_to_parse}")
    return files_to_parse


@task
def parse_save_dataframe(files, save_path, pattern):
    """
    Parse and save files from FTP
    """

    save_path = Path(save_path)
    save_path.mkdir(parents=True, exist_ok=True)

    if pattern == "ARQ2001":
        columns = {
            "orgao_transcritor": 9,
            "codigo_sici": 7,
            "numero_processo": 15,
            "tipo_processo": 12,
            "numero_documento_identidade": 15,
            "tipo_documento_identidade": 3,
            "descricao_tipo_documento_identidade": 31,
            "requerente": 51,
            "data_processo": 11,
            "codigo_assunto": 6,
            "descricao_assunto": 51,
            "data_cadastro": 11,
            "assunto_complementar": 225,
            "matricula_digitador": 9,
            "prazo_cadastro": 26,
        }
    elif pattern == "ARQ2296":
        columns = {
            "codigo_orgao": 8,
            "codigo_sici": 6,
            "tipo_documento": 2,
            "data_cadastro": 10,
            "numero_documento": 14,
            "requerente": 60,
            "codigo_assunto": 5,
            "descricao_assunto": 50,
            "data_tramitacao": 10,
            "orgao_destino": 8,
            "codigo_despacho": 5,
            "data_inicio": 10,
            "data_fim": 10,
            "orgao_inicio": 8,
            "orgao_fim": 8,
            "dias_parados": 6,
            "matricula_digitador": 8,
            "opcao": 1,
            "descricao_relatorio": 16,
            "filler": 3,
            "orgao_responsavel": 8,
            "informacao_complementar": 256,
        }
    else:
        raise "Pattern not found"

    table_columns = list(columns.keys())
    widths_columns = list(columns.values())

    for file in files:
        if file.stat().st_size != 0:
            dataframe = pd.read_fwf(
                file,
                encoding="cp1251",
                widths=widths_columns,
                header=None,
            )
            dataframe.columns = table_columns

            for col in dataframe.columns:
                dataframe[col] = dataframe[col].astype(str).str.replace(";", "")

            file_original_name = str(file).split("/")[-1]

            data_hora = file_original_name.split("_")[1] + file_original_name.split(
                "_"
            )[2].replace(".TXT", "")
            dataframe.insert(0, "data_arquivo", data_hora)
            dataframe["data_arquivo"] = pd.to_datetime(dataframe["data_arquivo"])

            dataframe, date_partition_columns = parse_date_columns(
                dataframe=dataframe, partition_date_column="data_arquivo"
            )

            to_partitions(
                data=dataframe,
                partition_columns=date_partition_columns,
                savepath=save_path,
                data_type="csv",
            )
            log(f"parsed and saved: {file}")
        else:
            log(f"File has no data : {file}")

    return save_path
