"""
Flows for emd
"""


from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.emd.waze.tasks import (
    load_geometries,
    fecth_waze,
    normalize_data,
    upload_to_native_table,
)
from pipelines.emd.waze.schedules import every_five_minutes

# from pipelines.emd.template_pipeline.schedules import every_two_weeks

with Flow("EMD: Waze: Alertas") as flow:

    areas = load_geometries()

    res = fecth_waze(areas=areas)

    df = normalize_data(responses=res)

    upload_to_native_table(
        dataset_id="transporte_rodoviario_waze", table_id="alertas", dataframe=df
    )

flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = every_five_minutes
