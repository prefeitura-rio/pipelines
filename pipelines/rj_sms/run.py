# -*- coding: utf-8 -*-
# from pipelines.rj_sms.dump_api_vitai.flows import dump_vitai
# from pipelines.rj_sms.dump_azureblob_tpc.flows import dump_tpc
# from pipelines.rj_sms.dump_api_vitacare.flows import dump_vitacare
# from pipelines.utils.utils import run_local
#
# run_local(dump_vitacare)

from datetime import datetime
from pathlib import Path
import re
import shutil
import basedosdados as bd


def create_partitions(data_path: str | Path, partition_directory: str | Path):
    files = data_path.glob("*.csv")

    for file_name in files:
        date_str = re.search(r"\d{4}-\d{2}-\d{2}", str(file_name)).group()
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        ano_particao = parsed_date.strftime("%Y")
        mes_particao = parsed_date.strftime("%m")
        data_particao = parsed_date.strftime("%Y-%m-%d")

        output_directory = (
            partition_directory
            / f"ano_particao={int(ano_particao)}"
            / f"mes_particao={int(mes_particao)}"
            / f"data_particao={data_particao}"
        )

        output_directory.mkdir(parents=True, exist_ok=True)

        # Copy file to partition directory
        shutil.copy(file_name, output_directory)


data_path = Path("/home/thiagotrabach/tmp/data")
partition_directory = Path("./partition_directory")
shutil.rmtree(partition_directory, ignore_errors=True)
create_partitions(data_path, partition_directory)

dataset_id = "dataset_test"
table_id = "sms_test"
tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
table_exists = tb.table_exists(mode="staging")

if not table_exists:
    print(f"CREATING TABLE: {dataset_id}.{table_id}")
    tb.create(
        path=partition_directory,
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )
else:
    print(f"TABLE ALREADY EXISTS APPENDING DATA TO STORAGE: {dataset_id}.{table_id}")

    tb.append(filepath=partition_directory, if_exists="replace")
