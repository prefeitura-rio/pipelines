# -*- coding: utf-8 -*-
"""
Tasks for dump_api_prontuario_vitacare
"""

from datetime import (
    date,
    datetime,
    timedelta,
)

from prefect import task
import pandas as pd
import os
import csv
import shutil
from google.cloud import bigquery
import requests
import json
from pipelines.rj_sms.dump_api_prontuario_vitacare.constants import (
    constants as vitacare_constants,
)
from pipelines.utils.utils import log
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.rj_sms.tasks import (
    from_json_to_csv,
    add_load_date_column,
    save_to_file,
    create_partitions,
    cloud_function_request_patients,
)


@task
def rename_flow(table_id: str, ap: str):
    rename_current_flow_run_dataset_table.run(
        prefix="SMS Dump VitaCare: ", dataset_id=table_id, table_id=f"ap{ap}"
    )


@task
def build_url(ap: str, endpoint: str) -> str:
    url = f"{vitacare_constants.BASE_URL.value[ap]}{vitacare_constants.ENDPOINT.value[endpoint]}"  # noqa: E501
    log(f"URL built: {url}")
    return url


@task
def build_params(date_param: str = "today"):
    if date_param == "today":
        params = {"date": str(date.today())}
    elif date_param == "yesterday":
        params = {"date": str(date.today() - timedelta(days=1))}
    else:
        try:
            # check if date_param is a date string
            datetime.strptime(date_param, "%Y-%m-%d")
            params = {"date": date_param}
        except ValueError as e:
            raise ValueError("date_param must be a date string (YYYY-MM-DD)") from e

    log(f"Params built: {params}")
    return params


@task
def create_filename(table_id: str, ap: str) -> str:
    return f"{table_id}_ap{ap}"


@task
def fix_payload_column_order(filepath: str, table_id: str, sep: str = ";"):
    """
    Load a CSV file into a pandas DataFrame, keeping all column types as string,
    and reorder the columns in a specified order.

    Parameters:
    - filepath: str
        The file path of the CSV file to load.

    Returns:
    - DataFrame
        The loaded DataFrame with columns reordered.
    """
    columns_order = {
        "estoque_posicao": [
            "ap",
            "cnesUnidade",
            "nomeUnidade",
            "desigMedicamento",
            "atc",
            "code",
            "lote",
            "dtaCriLote",
            "dtaValidadeLote",
            "estoqueLote",
            "id",
            "_data_carga",
        ],
        "estoque_movimento": [
            "ap",
            "cnesUnidade",
            "nomeUnidade",
            "desigMedicamento",
            "atc",
            "code",
            "lote",
            "dtaMovimento",
            "tipoMovimento",
            "motivoCorrecao",
            "justificativa",
            "cnsProfPrescritor",
            "cpfPatient",
            "cnsPatient",
            "qtd",
            "id",
            "_data_carga",
        ],
    }

    # Specifying dtype as str to ensure all columns are read as strings
    df = pd.read_csv(filepath, sep=sep, dtype=str, encoding="utf-8")

    # Specifying the desired column order
    column_order = columns_order[table_id]

    # Reordering the columns
    df = df[column_order]

    df.to_csv(filepath, sep=sep, index=False, encoding="utf-8")

    log(f"Columns reordered for {filepath}")


@task
def save_data_to_file(
    data: str,
    file_folder: str,
    table_id: str,
    ap: str,
    add_load_date_to_filename: bool = False,
    load_date: str = None,
):
    file_path = save_to_file.run(
        data=data,
        file_folder=file_folder,
        file_name=f"{table_id}_ap{ap}",
        add_load_date_to_filename=add_load_date_to_filename,
        load_date=load_date,
    )

    with open(file_path, "r", encoding="UTF-8") as f:
        first_line = f.readline().strip()

    if first_line == "[]":
        log("The json content is empty.")
        return False
    else:
        csv_file_path = from_json_to_csv.run(input_path=file_path, sep=";")

        add_load_date_column.run(input_path=csv_file_path, sep=";")

        fix_payload_column_order.run(filepath=csv_file_path, table_id=table_id)

        return True


@task
def get_patients(context):
    log("Getting data from cloud function")
    list_cnes = vitacare_constants.CNES.value
    if context == "scheduled":
        url = vitacare_constants.URL_PACIENTES_AGENDADOS.value
        data = datetime.today() + timedelta(days=3)
    else:
        url = vitacare_constants.URL_PACIENTES_ATENDIDOS.value
        data = datetime.today() - timedelta(days=1)
    data_formatada = data.strftime("%Y-%m-%d")
    df = pd.DataFrame()
    list_cnes_error = []
    list_cnes_empty = []
    for cnes in list_cnes:
        params = '{"cnes": "' + cnes + '", "date": "' + data_formatada + '"}'
        response = cloud_function_request_patients.run(
            url=url, request_type="POST", body_params=params, env="staging"
        )
        if response.text.startswith("A solicitação não foi bem-sucedida"):
            list_cnes_error.append(cnes)
        else:
            try:
                df_temp = pd.read_json(response.text)
            except:
                log(f"Error cnes - {cnes}, Detail: {response.text}", level="error")
            if not df_temp.empty:
                df = pd.concat([df, df_temp], ignore_index=True)
            else:
                list_cnes_empty.append(cnes)
    log(f"List cnes error {list_cnes_error}", level="error")
    log(f"List cnes empty erro {list_cnes_empty}", level="error")
    return df


@task
def save_patients(dataframe, context):
    log("Saving data into the server")
    path = "pipelines/rj_sms/dump_api_prontuario_vitacare/data"
    try:
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
            os.mkdir(path)
        else:
            os.mkdir(path)
        if context == "scheduled":
            data_futura = datetime.today() + timedelta(days=3)
        else:
            data_futura = datetime.today() - timedelta(days=1)
        data_formatada = data_futura.strftime("%Y-%m-%d")
        filename = (
            f"pipelines/rj_sms/dump_api_prontuario_vitacare/data/{data_formatada}.csv"
        )
        dataframe.to_csv(
            filename,
            sep=";",
            quoting=csv.QUOTE_NONNUMERIC,
            quotechar='"',
            index=False,
            encoding="utf-8",
        )
        partition_directory = (
            "pipelines/rj_sms/dump_api_prontuario_vitacare/data_partition"
        )
        shutil.rmtree(partition_directory, ignore_errors=True)
        create_partitions.run(
            "pipelines/rj_sms/dump_api_prontuario_vitacare/data", partition_directory
        )
        return True
    except:
        log("Error when trying to save files", level="error")
        return False


@task
def read_data(cnes=None, date_param=None, table=None):
    """
    Read data
    Args:
        date_param (str, mandatory): The date to query in the format "YYYY-MM-DD".
        cnes (str, mandatory): health unit identifier
        table (str, mandatory): Name of table on bigQuery
    Returns:
        str: Data from BigQuery.
    """
    client = bigquery.Client()

    query = f"""
        SELECT *
        FROM `{table}`
        WHERE cnes = '{cnes}' AND date = '{date_param}'
    """

    query_job = client.query(query)
    results = query_job.result()

    df = pd.DataFrame(
        data=[list(row.values()) for row in results],
        columns=list(results.schema.field_names),
    )

    return df


@task
def remove_opt_out(data=None):
    """
    Read table data "opt-out" and remove from data param
    Args:
        data (str, mandatory): List of scheduled patients.
    Returns:
        DataFrame: New data after remover opt-out.
    """
    client = bigquery.Client()

    query = """
        SELECT DISTINCT cpf
        FROM `rj-sms-dev.whatsapp_staging.opt_out`
    """

    # Executando a query e obtendo os CPFs
    query_job = client.query(query)
    cpf_opt_out = [row["cpf"] for row in query_job.result()]

    data_filtered = data[~data["cpf"].isin(cpf_opt_out)]
    return data_filtered


@task
def clean_data(data=None):
    """
    Remove empty and invalid numbers/ remove minors
    Args:
        data (str, mandatory): List of scheduled patients.
    Returns:
        DataFrame: New data after remover opt-out.
    """
    # Remover linhas com números de telefone inválidos ou vazios
    data = data[data["telefone"].astype(str).str.len() == 11]

    # Converter a coluna 'data_nascimento' para o tipo datetime
    data["data_nascimento"] = pd.to_datetime(data["data_nascimento"], errors="coerce")

    # Calcular a idade com base na data de nascimento
    data["idade"] = (datetime.now() - data["data_nascimento"]).astype("<m8[Y]")

    # Remover linhas com idade menor que 18 anos
    data = data[data["idade"] >= 18]

    # Remover coluna 'idade' temporária
    data = data.drop(columns=["idade"])


@task
def find_team_number():
    """
    Use the link to search for the team number it belongs to, passing the address as a parameter
    https://subpav.org/SAP/includes/
    Args:
        data (str, mandatory): List of scheduled patients.
    Returns:
        DataFrame: New data after remover opt-out.
    """
    return None


@task
def send_whatsapp(data: None, case: None):
    """
    Send message using whatsapp API Wetalkie
    Args:
        data (str, mandatory): List of scheduled patients.
        case (str, mandatory): Tipo de caso a ser execudado. Os valores podem ser:
            clinica_familia_scheduled_patients
            clinica_familia_patients_treated
            sisreg_scheduled_patients
    Returns:
        DataFrame: New data after remover opt-out.
    """
    for patient in data:
        if case == "clinica_familia_scheduled_patients":
            payload = json.dumps(
                [
                    {
                        "phone": patient["phone"],
                        "nome": patient["name"],
                        "procedimento": patient["procedimento"],
                        "data": patient["data"],
                        "horario": patient["horario"],
                        "unidade": patient["unidade"],
                        "endereco": patient["endereco"],
                        "urlcontato": patient["telefone_unidade"],
                    }
                ]
            )
            url = "https://takebroadcast.cs.blip.ai/api/v2/Broadcast/list?phoneColumn=phone&namespace=whatsapp%3Ahsm%3Amessaging%3Ablip&template=poc_sms_wa_72h_antes&flowId=4b96ec20-f0d1-48f9-8138-cd7d133e39ee&stateId=e738eeff-b394-4c29-8def-cef05a44ec40&scheduleTime=30&separator=%2C&checkAttendance=false"

        elif case == "clinica_familia_patients_treated":
            payload = json.dumps(
                [
                    {
                        "phone": patient["phone"],
                        "nome": patient["name"],
                        "data": patient["data"],
                        "unidade": patient["unidade"],
                    }
                ]
            )
            url = "https://takebroadcast.cs.blip.ai/api/v2/Broadcast/list?phoneColumn=phone&namespace=whatsapp%3Ahsm%3Amessaging%3Ablip&template=poc_sms_wa_24h_depois&flowId=4b96ec20-f0d1-48f9-8138-cd7d133e39ee&stateId=b27e0851-0f10-468c-be8b-186f00578058&scheduleTime=60&separator=%2C&checkAttendance=false"

        elif case == "sisreg_scheduled_patients":
            payload = json.dumps(
                [
                    {
                        "phone": patient["phone"],
                        "nome": patient["name"],
                        "procedimento": patient["procedimento"],
                        "especialidade": patient["especialidade"],
                        "preparo": patient["preparo"],
                        "data": patient["data"],
                        "horario": patient["horario"],
                        "unidade": patient["unidade"],
                        "endereco": patient["endereco"],
                        "urlcontato": patient["telefone_unidade"],
                    }
                ]
            )
            url = "https://takebroadcast.cs.blip.ai/api/v2/Broadcast/list?phoneColumn=phone&namespace=whatsapp%3Ahsm%3Amessaging%3Ablip&template=poc_sms_wa_5d_antes&flowId=4b96ec20-f0d1-48f9-8138-cd7d133e39ee&stateId=62af61cc-37b7-4cf9-ae81-ff7297399146&scheduleTime=60&separator=%2C&checkAttendance=false"

        headers = {
            "accept": "text/plain",
            "identifier": "@user",
            "accessKey": "@password",
            "Content-Type": "application/json-patch+json",
        }
        # print(f'Case: {case} - Payload whatsapp: {payload}')
        response = requests.request("POST", url, headers=headers, data=payload)
        save_log(response.text)
        print("Whatsapp Enviado:" + response.text)

    return "Mensagens do whatsapp enviadas"


def save_log():
    return None
