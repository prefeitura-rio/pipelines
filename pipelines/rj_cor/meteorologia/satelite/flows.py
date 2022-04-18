"""
Flows for emd
"""
import pendulum

from prefect import Flow, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.satelite.tasks import (slice_data, download,
                                                         tratar_dados, salvar_parquet)
from pipelines.rj_cor.meteorologia.satelite.schedules import hour_schedule
from pipelines.utils.tasks import (
    check_table_exists,
    create_bd_table,
    upload_to_gcs,
    dump_header_to_csv,
)

with Flow('COR: Meteorologia - Satelite') as flow:
    # CURRENT_TIME = Parameter('CURRENT_TIME', default=pendulum.now("utc"))
    CURRENT_TIME = pendulum.now('UTC')

    ano, mes, dia, hora, dia_juliano = slice_data(current_time=CURRENT_TIME)

   # Para taxa de precipitação
    VARIAVEL = 'RRQPEF'
    DATASET_ID = 'meio_ambiente_clima'
    TABLE_ID = 'satelite_taxa_precipitacao'
    DUMP_TYPE = 'append'

    filename = download(variavel=VARIAVEL,
                        ano=ano,
                        dia_juliano=dia_juliano,
                        hora=hora)
    info = tratar_dados(filename=filename)
    path, partitions = salvar_parquet(info=info)

    # Check if table exists
    EXISTS = check_table_exists(
       dataset_id=DATASET_ID,
       table_id=TABLE_ID
   )

    # Create header and table if they don't exists
    with case(EXISTS, False):
        # Create CSV file with headers
        header_path = dump_header_to_csv(
            data_path=path
        )

        # Create table in BigQuery
        create_db = create_bd_table(  # pylint: disable=invalid-name
            path=header_path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dump_type=DUMP_TYPE,
            wait=header_path,
        )
        # Upload to GCS
        upload_to_gcs(
            path=path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            partitions=partitions,
            wait=create_db,
        )

    with case(EXISTS, True):
        # Upload to GCS
        upload_to_gcs(
            path=path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            partitions=partitions,
        )



    # Para quantidade de água precipitável
    VARIAVEL = 'TPWF'
    DATASET_ID = 'meio_ambiente_clima'
    TABLE_ID = 'satelite_quantidade_agua_precipitavel'

    filename = download(variavel=VARIAVEL,
                        ano=ano,
                        dia_juliano=dia_juliano,
                        hora=hora)
    info = tratar_dados(filename=filename)
    path, partitions = salvar_parquet(info=info)

    # Check if table exists
    EXISTS = check_table_exists(
       dataset_id=DATASET_ID,
       table_id=TABLE_ID
   )

    # Create header and table if they don't exists
    with case(EXISTS, False):
        # Create CSV file with headers
        header_path = dump_header_to_csv(
            data_path=path
        )

        # Create table in BigQuery
        create_db = create_bd_table(  # pylint: disable=invalid-name
            path=header_path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dump_type=DUMP_TYPE,
            wait=header_path,
        )
        # Upload to GCS
        upload_to_gcs(
            path=path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            partitions=partitions,
            wait=create_db,
        )

    with case(EXISTS, True):
        # Upload to GCS
        upload_to_gcs(
            path=path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            partitions=partitions,
        )

# para rodar na cloud
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = hour_schedule
