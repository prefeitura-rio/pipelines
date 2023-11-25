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

from pipelines.rj_sms.dump_api_prontuario_vitacare.constants import constants as vitacare_constants
from pipelines.utils.utils import log
from pipelines.rj_sms.tasks import (
    from_json_to_csv,
    add_load_date_column,
    save_to_file
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
            datetime.datetime.strptime(date_param, '%Y-%m-%d')
            params = {"date": date_param}
        except ValueError:
            raise ValueError("date_param must be a date string (YYYY-MM-DD)")
    
    log(f"Params built: {params}")
    return params

@task
def create_filename(table_id: str, ap: str) -> str:
    return f"{table_id}_ap{ap}"

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
        load_date=load_date
    )

    with open(file_path, 'r', encoding="UTF-8") as f:
        first_line = f.readline().strip()

    if first_line == '[]':
        log("The json content is empty.")
        return False
    else:
        csv_file_path = from_json_to_csv.run(input_path=file_path, sep=";")

        add_load_date_column.run(input_path=csv_file_path, sep=";")
        return True

