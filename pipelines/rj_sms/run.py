#-*- coding: utf-8 -*-
from pipelines.rj_sms.dump_api_vitacare.flows import dump_vitacare
from pipelines.utils.utils import run_local

run_local(dump_vitacare)


#
#
#
#
#
#
#data_path = Path("/home/thiagotrabach/tmp/data")
#partition_directory = Path("./partition_directory")
#
#create_partitions(data_path, partition_directory)
#
#dataset_id = "dataset_test"
#table_id = "sms_test"
#tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
#table_exists = tb.table_exists(mode="staging")
#
#if not table_exists:
#    print(f"CREATING TABLE: {dataset_id}.{table_id}")
#    tb.create(
#        path=partition_directory,
#        csv_delimiter=";",
#        if_storage_data_exists="replace",
#        biglake_table=True,
#    )
#else:
#    print(f"TABLE ALREADY EXISTS APPENDING DATA TO STORAGE: {dataset_id}.{table_id}")
#
#    tb.append(filepath=partition_directory, if_exists="replace")