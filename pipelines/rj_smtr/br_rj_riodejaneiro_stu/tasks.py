# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_stu
"""
from io import StringIO
from prefect import task
import pandas as pd
from google.cloud.storage.blob import Blob
import basedosdados as bd
from pipelines.utils.utils import log
from pipelines.rj_smtr.constants import constants


@task(checkpoint=False)
def get_stu_raw_blobs(data_versao_stu: str) -> list[Blob]:
    """
    Get STU extraction files

    Args:
        data_versao_stu (str): The STU version date in the format YYYY-MM-DD

    Returns:
        list[Blob]: The blob list
    """
    bd_storage = bd.Storage(
        dataset_id=constants.STU_GENERAL_CAPTURE_PARAMS.value["dataset_id"],
        table_id="",
        bucket_name=constants.STU_GENERAL_CAPTURE_PARAMS.value["save_bucket_name"],
    )

    blob_list = (
        bd_storage.client["storage_staging"]
        .bucket(bd_storage.bucket_name)
        .list_blobs(prefix=f"upload/{bd_storage.dataset_id}/Tptran_")
    )

    blob_list = [
        b
        for b in blob_list
        if b.name.endswith(f"{data_versao_stu.replace('-', '')}.txt")
    ]

    log(f"Files found: {', '.join([b.name for b in blob_list])}")

    return blob_list


@task(checkpoint=False)
def read_stu_raw_file(blob: Blob) -> pd.DataFrame:
    """
    Read an extracted file from STU

    Args:
        blob (Blob): The GCS blob

    Returns:
        pd.DataFrame: data
    """

    log(f"Downloading blob: {blob.name}")
    data = blob.download_as_bytes().decode("latin-1")
    name_parts = blob.name.split("/")[-1].split("_")
    mode = constants.STU_MODE_MAPPING.value[name_parts[1]]
    perm_type = constants.STU_TYPE_MAPPING.value[int(name_parts[3]) - 1]

    df = pd.read_csv(
        StringIO(data),
        sep=";",
        decimal=",",
        encoding="latin-1",
        dtype="object",
    )

    df["modo"] = mode
    df["tipo_permissao"] = perm_type
    df.columns = [c.replace("/", " ").replace(" ", "_") for c in df.columns]

    return df


@task(checkpoint=False, nout=2)
def create_final_stu_dataframe(
    dfs: list[pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Join all dataframes according to the document type

    Args:
        dfs (list[pd.DataFrame]): The list of dfs from all stu files

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Dataframe for regular persons, dataframe for companies
    """
    dfs_pf = []
    dfs_pj = []

    for df in dfs:
        if "CPF" in df.columns:
            dfs_pf.append(df)
        elif "CNPJ" in df.columns:
            dfs_pj.append(df)
        else:
            raise ValueError("Document column not found")

    return pd.concat(dfs_pf), pd.concat(dfs_pj)


@task
def save_stu_dataframes(df_pf: pd.DataFrame, df_pj: pd.DataFrame):
    """
    Save STU concatenated dataframes into the upload folder

    Args:
        df_pf (pd.DataFrame): Dataframe for regular persons
        df_pj (pd.DataFrame): Dataframe for companies
    """

    df_mapping = {"operadora_pessoa_fisica": df_pf, "operadora_empresa": df_pj}
    bd_storage = bd.Storage(
        table_id="",
        dataset_id=constants.STU_GENERAL_CAPTURE_PARAMS.value["dataset_id"],
        bucket_name=constants.STU_GENERAL_CAPTURE_PARAMS.value["save_bucket_name"],
    )

    bucket = bd_storage.client["storage_staging"].bucket(bd_storage.bucket_name)

    for table in constants.STU_TABLE_CAPTURE_PARAMS.value:
        table_id = table["table_id"]
        df = df_mapping[table_id]
        bucket.blob(
            f"upload/{bd_storage.dataset_id}/{table_id}.csv"
        ).upload_from_string(df.to_csv(index=False), "text/csv")
