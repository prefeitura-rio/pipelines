"""
General purpose functions for the geolocator project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto geolocator.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar funções, basta fazer em código Python comum, como abaixo:
#
# ```
# def foo():
#     """
#     Function foo
#     """
#     print("foo")
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.rj_escritorio.geolocator.utils import foo
# foo()
# ```
#
###############################################################################
import requests


def geolocator(q: str) -> list:
    """
    Utiliza a api do waze pra geolocalizar (lat/long) um endereço.
    """

    url = f"https://gapi.waze.com/autocomplete/q?q={q}&e=ALL&c=web&gxy=1&exp=8%2C10%2C12&v=-22.9370135%2C-43.18795323%3B-22.9301167%2C-43.1815052&lang=pt"

    payload = {}
    headers = {}
    try:
        response = requests.request("GET", url, headers=headers, data=payload)

        long = response.json()[1][0][3]["x"]
        lat = response.json()[1][0][3]["y"]
        return [lat, long]
    except:
        return [None, None]
