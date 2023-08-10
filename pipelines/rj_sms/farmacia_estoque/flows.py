from prefect import Flow, Parameter
from pipelines.utils.utils import(
    get_vault_secret,
)
from tasks import (
    download_blob,
)
    
with Flow(
    "SMS: Farmacia - Captura de dados TPC",
    code_owners=["thiago", "andre"]
) as captura_tpc:
    
    connection_string = get_vault_secret("estoque_tpc")
    print(connection_string)
    #container_name = Parameter("container_name", default="tpc")
    #blob_name = Parameter("blob_name", default="report.csv")
    #destination_file_path = f"/home/thiagotrabach/projects/poc-pipeline/data/{blob_name}"

    #download_blob(connection_string, container_name, blob_name, destination_file_path)