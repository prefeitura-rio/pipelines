# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows for emd
"""
from copy import deepcopy

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.satelite.constants import (
    constants as satelite_constants,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.rj_cor.meteorologia.satelite.tasks import (
    create_image_and_upload_to_api,
    get_dates,
    slice_data,
    download,
    tratar_dados,
    save_data,
)
from pipelines.rj_cor.tasks import (
    get_on_redis,
    save_on_redis,
)
from pipelines.rj_cor.meteorologia.satelite.schedules import (
    cmip,
    mcmip,
    rrqpe,
    tpw,
    dsi,
    lst,
    sst,
    aod,
)

from pipelines.utils.decorators import Flow

from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
)

with Flow(
    name="COR: Meteorologia - Satelite GOES 16",
    code_owners=[
        "paty",
    ],
) as cor_meteorologia_goes16:
    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    materialization_mode = Parameter("mode", default="dev", required=False)

    # Other parameters
    dataset_id = satelite_constants.DATASET_ID.value
    band = Parameter("band", default=None, required=False)()
    product = Parameter("product", default=None, required=False)()
    table_id = Parameter("table_id", default=None, required=False)()
    dump_mode = "append"
    mode_redis = Parameter("mode_redis", default="prod", required=False)
    ref_filename = Parameter("ref_filename", default=None, required=False)
    current_time = Parameter("current_time", default=None, required=False)
    create_image = Parameter("create_image", default=False, required=False)

    # Starting tasks
    current_time = get_dates(current_time, product)

    date_hour_info = slice_data(current_time=current_time, ref_filename=ref_filename)

    # # Get filenames that were already treated on redis
    redis_files = get_on_redis(dataset_id, table_id, mode=mode_redis)
    # redis_files = []

    # Download raw data from API
    filename, redis_files_updated = download(
        product=product,
        date_hour_info=date_hour_info,
        band=band,
        redis_files=redis_files,
        ref_filename=ref_filename,
        wait=redis_files,
        mode_redis=mode_redis,
    )

    # Start data treatment if there are new files
    info = tratar_dados(filename=filename)
    path, output_filepath = save_data(info=info, mode_redis=mode_redis)

    # Create table in BigQuery
    upload_table = create_table_and_upload_to_gcs(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        wait=path,
    )

    # Save new filenames on redis
    save_on_redis(
        dataset_id,
        table_id,
        mode_redis,
        redis_files_updated,
        wait=path,
    )

    with case(create_image, True):
        create_image_and_upload_to_api(info, output_filepath)

    # Trigger DBT flow run
    with case(materialize_after_dump, True):
        current_flow_labels = get_current_flow_labels()

        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )

        materialization_flow.set_upstream(upload_table)

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )


# para rodar na cloud
# cor_meteorologia_goes16.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# cor_meteorologia_goes16.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value,
#     labels=[constants.RJ_COR_AGENT_LABEL.value],
# )
cor_meteorologia_goes16_rrqpe = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_rrqpe.name = "COR: Meteorologia - Satelite GOES 16 - RRQPE"
cor_meteorologia_goes16_rrqpe.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_rrqpe.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_rrqpe.schedule = rrqpe

cor_meteorologia_goes16_tpw = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_tpw.name = "COR: Meteorologia - Satelite GOES 16 - TPW"
cor_meteorologia_goes16_tpw.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_tpw.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_tpw.schedule = tpw

cor_meteorologia_goes16_cmip = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_cmip.name = "COR: Meteorologia - Satelite GOES 16 - CMIP"
cor_meteorologia_goes16_cmip.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_cmip.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_cmip.schedule = cmip

cor_meteorologia_goes16_mcmip = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_mcmip.name = "COR: Meteorologia - Satelite GOES 16 - MCMIP"
cor_meteorologia_goes16_mcmip.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_mcmip.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_mcmip.schedule = mcmip

cor_meteorologia_goes16_dsi = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_dsi.name = "COR: Meteorologia - Satelite GOES 16 - DSI"
cor_meteorologia_goes16_dsi.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_dsi.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_dsi.schedule = dsi

cor_meteorologia_goes16_lst = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_lst.name = "COR: Meteorologia - Satelite GOES 16 - LST"
cor_meteorologia_goes16_lst.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_lst.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_lst.schedule = lst

cor_meteorologia_goes16_sst = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_sst.name = "COR: Meteorologia - Satelite GOES 16 - SST"
cor_meteorologia_goes16_sst.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_sst.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_sst.schedule = sst

cor_meteorologia_goes16_aod = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_aod.name = "COR: Meteorologia - Satelite GOES 16 - AOD"
cor_meteorologia_goes16_aod.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_aod.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_aod.schedule = aod
