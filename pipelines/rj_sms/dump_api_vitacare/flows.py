# -*- coding: utf-8 -*-
from prefect import Parameter
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.rj_sms.utils import (
    create_folders,
    from_json_to_csv,
    download_api, )
from pipelines.rj_sms.dump_api_vitacare.tasks import (
    build_params,
)
from pipelines.rj_sms.dump_api_vitacare.scheduler import every_day_at_six_am

with Flow(
    name="SMS: Dump VitaCare - Captura de dados VitaCare", code_owners=["thiago"]
) as dump_vitacare:
    # Set Parameters
    #  Vault
    vault_path = None
    vault_key = None
    #  GCP
    dataset_id = "dump_vitacare"
    table_id = "estoque_posicao"
    dump_mode = Parameter("dump_mode", default="append")  # append / overwrite

    # Start run
    create_folders_task = create_folders()

    build_params_task = build_params()
    build_params_task.set_upstream(create_folders_task)

    download_task = download_api(
        url="http://consolidado-ap10.pepvitacare.com:8088/reports/pharmacy/stocks",
        params=build_params_task,
        destination_file_name=table_id,
        vault_path=vault_path,
        vault_key=vault_key,
        add_load_date_to_filename=True,
    )
    download_task.set_upstream(build_params_task)

    #conversion_task = from_json_to_csv(input_path=download_task, sep=";")
    #conversion_task.set_upstream(download_task)
#
    ## partition task
    #create_partitions_task = create_partitions
    ## upload to datalake


dump_vitacare.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitacare.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitacare.schedule = every_day_at_six_am
