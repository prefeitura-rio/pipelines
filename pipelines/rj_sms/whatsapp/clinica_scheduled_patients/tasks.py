# -*- coding: utf-8 -*-
import os
import csv
import time
import json
import requests
import pandas as pd
from prefect import task
from pipelines.utils.utils import log
#from google.cloud import storage
from datetime import datetime, timedelta
import google.oauth2.id_token
import google.auth.transport.requests

@task
def get_patients():
    url = 'https://us-central1-rj-sms-dev.cloudfunctions.net/vitacare'

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/andremartins/.basedosdados/credentials/staging.json'
    request = google.auth.transport.requests.Request()
    audience = url
    TOKEN = google.oauth2.id_token.fetch_id_token(request, audience)

    payload = json.dumps({
    "url": "http://homologacao-devrj.pepvitacare.com:9003/health/schedule/nextappointments",
    "path": "whatsapp/clinica_patients_treated/origin/",
    "env": "staging"
    })
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {TOKEN}'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    log(response.text)

@task
def save_patients(dataframe):
    data_futura = datetime.today() + timedelta(days=3)
    data_formatada = data_futura.strftime('%Y-%m-%d')
    #filename = f'sisreg_scheduled_patients/origin/{data_formatada}.csv'
    filename = f'files/{data_formatada}.csv'
    #bucket_name = 'rj-whatsapp'
    #storage_client = storage.Client()
    #bucket = storage_client.get_bucket(bucket_name)
    #blob = bucket.blob(filename)
    #csv_data = dataframe.to_csv(sep=';', quoting=csv.QUOTE_NONNUMERIC, quotechar='"', index=False, encoding='utf-8')
    dataframe.to_csv(filename, sep=';', quoting=csv.QUOTE_NONNUMERIC, quotechar='"', index=False, encoding='utf-8')
    #blob.upload_from_string(csv_data, content_type="text/csv")
    return True
