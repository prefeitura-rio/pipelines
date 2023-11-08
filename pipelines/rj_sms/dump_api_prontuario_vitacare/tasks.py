# -*- coding: utf-8 -*-
"""
Tasks for dump_api_prontuario_vitacare
"""

from datetime import date, timedelta
import re
import json
import requests
import google.oauth2.id_token
import google.auth.transport.requests
from prefect import task
from pipelines.utils.utils import log
from pipelines.rj_sms.utils import (
    from_json_to_csv,
    download_from_api,
    add_load_date_column
)


@task
def build_params(date_param="today"):
    if date_param == "today":
        params = {"date": str(date.today())}
    elif date_param == "yesterday":
        params = {"date": str(date.today() - timedelta(days=1))}
    else:
        {"date": date_param}
    log(f"Params built: {params}")
    return params


@task(max_retries=3, retry_delay=timedelta(seconds=5), timeout=timedelta(seconds=600))
def download_multiple_files(
    base_urls: list, endpoint: str, params: dict, table_id:str, vault_path: str, vault_key: str
):
    
    pattern = r"ap\d{2}"
    
    for n, base_url in enumerate(base_urls):
        try:
            ap = re.findall(pattern, base_url)[0]

            log(f"Downloading {ap} ({n+1}/{len(base_urls)})")

            download_task = download_from_api.run(
                url=f"{base_url}{endpoint}",
                params=params,
                file_folder="./data/raw",
                file_name=f"{table_id}-{ap}",
                vault_path=vault_path,
                vault_key=vault_key,
                add_load_date_to_filename=True,
                load_date=params["date"],
            )

            #
            with open(download_task, 'r', encoding="UTF-8") as f:
                first_line = f.readline().strip()
            if first_line == '[]':
                log("The json content is empty.")
            else:
                conversion_task = from_json_to_csv.run(input_path=download_task, sep=";")

                add_load_date_column_task = add_load_date_column.run(
                    input_path=conversion_task, sep=";"
                )
        except Exception as e:
            log(f"Error downloading {ap}. {e}", level="error")



@task
def download_to_cloudstorage():

    url = 'https://us-central1-rj-sms-dev.cloudfunctions.net/vitacare-v2'

    request = google.auth.transport.requests.Request()
    TOKEN = google.oauth2.id_token.fetch_id_token(request, url)

    payload = json.dumps({
        "url": "http://consolidado-ap10.pepvitacare.com:8088/reports/pharmacy/movements?date=2023-10-28&cnes=4030990",
        "path": "temporary_downloads/",
        "env": "staging"
    })
    headers = {
        'Content-Type': 'application/json',
}
    response = requests.request("POST", url, headers=headers, data=payload)
    log(response.text)