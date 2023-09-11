# -*- coding: utf-8 -*-
from prefect import Parameter
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)
from pipelines.rj_sms.utils import (
    download_api,
    from_json_to_csv,
)
from pipelines.rj_sms.dump_api_vitacare.tasks import conform_csv_to_gcp, upload_to_datalake
from pipelines.rj_sms.dump_api_vitacare.scheduler import every_day_at_six_am
from datetime import date


with Flow(
    name="SMS: Dump VitaCare - Captura de dados VitaCare", code_owners=["thiago"]
) as dump_vitacare:
    # Set Parameters
    #  Vault
    vault_path = ""
    vault_key = ""
    # VitaCare
    date = Parameter("date", default = str(date.today())) # format YYYY-MM-DD
    #  GCP
    dataset_id = "dump_vitacare"
    table_id = "estoque_posicao"
    dump_mode = Parameter("dump_mode", default="append")  # append / overwrite

    # Start run
    download_task = download_api(
        url=f"http://consolidado-ap10.pepvitacare.com:8088/reports/pharmacy/stocks?date={date}",
        destination_file_name=table_id,
        vault_path=vault_path,
        vault_key=vault_key,
    )

    

    conversion_task = from_json_to_csv(input_path=download_task, sep=";")
    conversion_task.set_upstream(download_task)

    conform_task = conform_csv_to_gcp(input_path=conversion_task)
    conform_task.set_upstream(conversion_task)

    upload_task = upload_to_datalake(
        input_path = conform_task,
        dataset_id = dataset_id,
        table_id = table_id)
    upload_task.set_upstream(conform_task)
    #upload_task = create_table_and_upload_to_gcs(
    #    data_path=conform_task,
    #    dataset_id=dataset_id,
    #    table_id=table_id,
    #    dump_mode=dump_mode,
    #    biglake_table=True,
    #    wait=None,
    #)
    #upload_task.set_upstream(conform_task)


dump_vitacare.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitacare.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitacare.schedule = every_day_at_six_am
