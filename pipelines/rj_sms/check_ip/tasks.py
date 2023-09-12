# -*- coding: utf-8 -*-
import requests
from prefect import task
from pipelines.utils.utils import log


@task
def get_public_ip():
    try:
        # Use a public IP address API to fetch your IP address
        response = requests.get("https://api64.ipify.org?format=json")

        if response.status_code == 200:
            data = response.json()
            log(f"IP: {data['ip']}")
            return data["ip"]
        else:
            log(f"Failed to retrieve IP address. Status Code: {response.status_code}")
    except Exception as e:
        log(f"An error occurred: {str(e)}")

    return None
