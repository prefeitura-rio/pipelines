# -*- coding: utf-8 -*-
import csv
import time
import json
import requests
import pandas as pd
from prefect import task
#from google.cloud import storage
from utils import log
from datetime import datetime, timedelta

@task
def get_patients():
    # Get Autentication
    url = "https://rest.smsrio.org/api/usuario/autenticar"

    payload = json.dumps({
        "cpf": "SisregAmb",
        "senha": "77HtOzVJ6^#d",
        "cnes": "5462886"
    })

    headers = {
        'Content-Type': 'application/json',
        'Cookie': 'PHPSESSID=b40302ab232addf99960f1d4ffa7073b'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200: 
        # Get token
        dados_json = json.loads(response.text)
        token = dados_json['dados']
        data_futura = datetime.today() + timedelta(days=3)
        data_formatada = data_futura.strftime('%Y-%m-%d')
        # Config Parameter
        list_cnes = ["6688152"]
        for cnes in list_cnes:
            url = f"https://web2.smsrio.org/ambulatorio/api/pacientesAgendados/{cnes}/{data_formatada}/"

            payload = ""
            headers = {
                'Authorization': 'Bearer ' + token
            }

            # Desired number of repetitions
            num_repeticoes = 5

            for _ in range(num_repeticoes):
                response = requests.get(url, headers=headers, data=payload)
                if response.status_code == 200:
                    log("Solicitação bem-sucedida!")
                    df = pd.read_json(response.text)
                    if df.empty:
                        log('DataFrame is empty!')
                    else:
                       return df 
                    break
                else:
                    log(f"Falha na solicitação, código de status: {response.status_code}")

                # Aguarda 1 minuto antes da próxima solicitação
                time.sleep(60)
                
    else:
        log('Erro na autenticação')
    
    return pd.DataFrame()

@task
def save_patients(dataframe):
    log('Salva lista de pacientes no cloud storage')
    data_futura = datetime.today() + timedelta(days=3)
    data_formatada = data_futura.strftime('%Y-%m-%d')
    #filename = f'sisreg_scheduled_patients/origin/{data_formatada}.csv'
    filename = f'files/{data_formatada}.csv'
    #bucket_name = 'rj-whatsapp'
    #storage_client = storage.Client()
    #bucket = storage_client.get_bucket(bucket_name)
    #blob = bucket.blob(filename)
    #csv_data = dataframe.to_csv(sep=';', quoting=csv.QUOTE_NONNUMERIC, quotechar='"', index=False, encoding='utf-8')
    #dataframe.to_csv(filename, sep=';', quoting=csv.QUOTE_NONNUMERIC, quotechar='"', index=False, encoding='utf-8')
    #blob.upload_from_string(csv_data, content_type="text/csv")
    return True