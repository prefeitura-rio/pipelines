import csv
import json
import requests
import pandas as pd
import functions_framework
from google.cloud import storage
from datetime import datetime, timedelta

@functions_framework.http
def vitacare(request):
    
    request_json = request.get_json(silent=True)
    request_args = request.args

    # Obtendo parametros da request
    url = request_json.get('url') or request_args.get('url')
    bucket_name = request_json.get('bucket') or request_args.get('bucket')
    path = request_json.get('path') or request_args.get('path')
    
    if not url or not bucket_name or not path:
        return "Certifique-se de fornecer os parâmetros 'url', 'bucket' e 'path' na solicitação."

    result = process_request(url, path, bucket_name)
    if isinstance(result, pd.DataFrame):
        return save_cloud_storage(result, bucket_name, path)
    else:
        return result


def process_request(url, path, bucket_name):
    if bucket_name == 'rj_whatsapp':
        if path == 'clinica_scheduled_patients/origin/':
            data = datetime.today() + timedelta(days=3)
        elif path == 'clinica_patients_treated/origin/':
            data = datetime.today() - timedelta(days=1)
        else:
            return 'Path unknown' 
        data_formatada = data.strftime('%Y-%m-%d')
        list_cnes = ["2269376"]
        headers = {
            'Content-Type': 'application/json'
        }
        for cnes in list_cnes:
            payload = json.dumps({
                "cnes": cnes,
                "date": data_formatada
            })
            response = requests.request("GET", url, headers=headers, data=payload)    
            if response.status_code != 200:
                return f"A solicitação não foi bem-sucedida. Código de status: {response.status_code}"

            try:
                json_data = response.json()
                df = pd.DataFrame(json_data)
                if df.empty:
                    return('DataFrame is empty!')
                else:
                    return df
            except ValueError:
                return "Falha ao analisar os dados JSON."
    else:
        response = requests.get(url)

        if response.status_code != 200:
            return f"A solicitação não foi bem-sucedida. Código de status: {response.status_code}"

        try:
            data = response.json()
            df = pd.DataFrame(data)
            return df
        except ValueError:
            return "Falha ao analisar os dados JSON."


def save_cloud_storage(df, bucket_name, path):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    if bucket_name == 'rj_whatsapp':
        if path == 'clinica_scheduled_patients/origin/':
            data = datetime.today() + timedelta(days=3)
        elif path == 'clinica_patients_treated/origin/':
            data = datetime.today() - timedelta(days=1)
        nome_arquivo = path + data + '.csv'
    else:
        nome_arquivo = path + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + ".csv"
    blob = bucket.blob(nome_arquivo)
    csv_data = df.to_csv(sep=';', quoting=csv.QUOTE_NONNUMERIC, quotechar='"', index=False, encoding='utf-8')
    blob.upload_from_string(csv_data, content_type="text/csv")

    return f"Arquivo CSV salvo em gs://{bucket_name}/{nome_arquivo}"
