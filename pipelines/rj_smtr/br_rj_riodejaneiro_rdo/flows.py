# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_rdo
"""

###############################################################################
#
# Aqui é onde devem ser definidos os flows do projeto.
# Cada flow representa uma sequência de passos que serão executados
# em ordem.
#
# Mais informações sobre flows podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/flows.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# Existem diversas maneiras de declarar flows. No entanto, a maneira mais
# conveniente e recomendada pela documentação é usar a API funcional.
# Em essência, isso implica simplesmente na chamada de funções, passando
# os parâmetros necessários para a execução em cada uma delas.
#
# Também, após a definição de um flow, para o adequado funcionamento, é
# mandatório configurar alguns parâmetros dele, os quais são:
# - storage: onde esse flow está armazenado. No caso, o storage é o
#   próprio módulo Python que contém o flow. Sendo assim, deve-se
#   configurar o storage como o pipelines.rj_smtr
# - run_config: para o caso de execução em cluster Kubernetes, que é
#   provavelmente o caso, é necessário configurar o run_config com a
#   imagem Docker que será usada para executar o flow. Assim sendo,
#   basta usar constants.DOCKER_IMAGE.value, que é automaticamente
#   gerado.
# - schedule (opcional): para o caso de execução em intervalos regulares,
#   deve-se utilizar algum dos schedules definidos em schedules.py
#
# Um exemplo de flow, considerando todos os pontos acima, é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
# from prefect import Flow
# from prefect.run_configs import KubernetesRun
# from prefect.storage import GCS
# from pipelines.constants import constants
# from my_tasks import my_task, another_task
# from my_schedules import some_schedule
#
# with Flow("my_flow") as flow:
#     a = my_task(param1=1, param2=2)
#     b = another_task(a, param3=3)
#
# flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = some_schedule
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

# from copy import deepcopy
from prefect import Parameter, case
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.tasks import (
    get_file_paths_from_ftp,
    download_and_save_local_from_ftp,
    pre_treatment_br_rj_riodejaneiro_rdo,
    get_rdo_date_range,
)
from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.tasks import (
    bq_upload,
    get_current_timestamp,
    set_last_run_timestamp,
)
from pipelines.rj_smtr.schedules import ftp_schedule

# from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.schedules import every_two_weeks
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client
from pipelines.utils.tasks import (
    # get_now_time,
    rename_current_flow_run_now_time,
    get_current_flow_mode,
    get_current_flow_labels,
)
from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

with Flow("SMTR - SPPO RHO - Materialização") as rho_mat_flow:
    # Rename flow run
    # rename_flow_run = rename_current_flow_run_now_time(
    #     prefix="SPPO RHO - Materialização: ", now_time=get_now_time()
    # )

    # Get default parameters #
    dataset_id = Parameter("dataset_id", default=constants.RDO_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.SPPO_RHO_TABLE_ID.value)
    rebuild = Parameter("rebuild", False)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode=MODE)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Set specific run parameters #
    date_range = get_rdo_date_range(dataset_id=dataset_id, table_id=table_id, mode=MODE)
    # Run materialization #
    with case(rebuild, True):
        RUN = run_dbt_model(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            table_id=table_id,
            upstream=True,
            _vars=[date_range],
            flags="--full-refresh",
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )
    with case(rebuild, False):
        RUN = run_dbt_model(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            table_id=table_id,
            _vars=[date_range],
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )

with Flow(
    "SMTR - RDO -Captura FTP",
    code_owners=["caio", "fernanda"],
) as captura_ftp:
    # SETUP
    transport_mode = Parameter("transport_mode", "SPPO")
    report_type = Parameter("report_type", "RHO")
    dump = Parameter("dump", False)
    table_id = Parameter("table_id", constants.SPPO_RHO_TABLE_ID.value)

    rename_run = rename_current_flow_run_now_time(
        prefix=f"Captura FTP - {transport_mode}-{report_type} ",
        now_time=get_current_timestamp(),
        wait=None,
    )
    # EXTRACT
    files = get_file_paths_from_ftp(
        transport_mode=transport_mode, report_type=report_type, dump=dump
    )
    updated_info = download_and_save_local_from_ftp.map(file_info=files)
    # TRANSFORM
    treated_path, raw_path, partitions = pre_treatment_br_rj_riodejaneiro_rdo(
        files=updated_info
    )
    # LOAD
    error = bq_upload.map(
        dataset_id=unmapped(constants.RDO_DATASET_ID.value),
        table_id=unmapped(table_id),
        filepath=treated_path,
        raw_filepath=raw_path,
        partitions=partitions,
    )
    with case(bool(error), False):
        RUN = create_flow_run(
            flow_name=rho_mat_flow.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            run_name=rho_mat_flow.name,
        )
        wait_for_flow_run(
            RUN,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

captura_ftp.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_ftp.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
captura_ftp.schedule = ftp_schedule


# captura_sppo_rho = deepcopy(captura_sppo_rdo)
# captura_sppo_rho.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
# captura_sppo_rho.run_config = KubernetesRun(image=emd_constants.DOCKER_IMAGE.value)
