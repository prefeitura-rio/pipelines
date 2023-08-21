# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log, get_vault_secret
from azure.storage.blob import BlobServiceClient
import os
import pandas as pd
import requests
from datetime import date


@task
def download_azure_blob(container_name, blob_name, destination_file_path):
    """
    Download a blob from Azure Blob Storage to a local file.

    :param connection_string: Azure Blob Storage connection string
    :param container_name: Name of the container where the blob is located
    :param blob_name: Name of the blob to download
    :param destination_folder_path: Local folder path to save the downloaded blob
    """
    credential = get_vault_secret(secret_path="estoque_tpc")["data"]["credential"]

    blob_service_client = BlobServiceClient(
        account_url="https://datalaketpcgen2.blob.core.windows.net/",
        credential=credential,
    )
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )

    with open(destination_file_path, "wb") as blob_file:
        blob_data = blob_client.download_blob()
        blob_data.readinto(blob_file)

    log(f"Blob '{blob_name}' downloaded to '{destination_file_path}'.")


@task
def download_api(url: str, destination_file_path, source: str):
    if source == "vitai":
        auth_token = get_vault_secret(secret_path="estoque_vitai")["data"]["token"]
    else:
        raise Exception("Source must be filled")
    headers = {"Authorization": f"Bearer {auth_token}"}

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            df = pd.DataFrame(response.jon())
            df["_data_carga"] = date.today()
            df.to_csv(
                destination_file_path,
                index=False,
                sep=";",
                encoding="utf-8",
                quoting=0,
                decimal=".",
            )

        else:
            log("Error:", response.status_code, response.text)

    except requests.exceptions.RequestException as e:
        log("An error occurred:", e)


@task
def set_destination_file_path(file):
    return (
        os.path.expanduser("~")
        + "/"
        + file[: file.find(".")]
        + "_"
        + str(date.today())
        + file[file.find(".") :]
    )


@task
def fix_payload_tpc(filepath: str):
    df = pd.read_csv(filepath, sep=";", keep_default_na=False)

    # remove registros errados
    df = df[df.sku != ""]

    # remove caracteres que confundem o parser
    df["item_nome_longo"] = df.item_nome_longo.apply(lambda x: x.replace('"', ""))
    df["item_nome_longo"] = df.item_nome_longo.apply(lambda x: x.replace(",", ""))
    df["item_nome"] = df.item_nome_longo.apply(lambda x: x.replace(",", ""))

    # converte para valores numéricos
    df["volume"] = df.volume.apply(lambda x: float(x.replace(",", ".")))
    df["peso_bruto"] = df.peso_bruto.apply(lambda x: float(x.replace(",", ".")))
    df["qtd_dispo"] = df.qtd_dispo.apply(lambda x: float(x.replace(",", ".")))
    df["qtd_roma"] = df.qtd_roma.apply(lambda x: float(x.replace(",", ".")))

    # converte as validades
    df["validade"] = df.validade.apply(lambda x: x[:10])
    df["dt_situacao"] = df.dt_situacao.apply(
        lambda x: x[-4:] + "-" + x[3:5] + "-" + x[:2]
    )

    df.to_csv(filepath, index=False, sep=",", encoding="utf-8", quoting=0, decimal=".")


@task
def fix_payload_vitai(filepath: str):
    df = pd.read_csv(filepath, sep=";", keep_default_na=False)

    # remove caracteres que confundem o parser
    df["descricao"] = df.descricao.apply(lambda x: x.replace('"', ""))
    df["descricao"] = df.descricao.apply(lambda x: x.replace(",", ""))

    df.to_csv(filepath, index=False, sep=",", encoding="utf-8", quoting=0, decimal=".")
