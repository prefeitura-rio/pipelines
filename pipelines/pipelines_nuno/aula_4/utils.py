# -*- coding: utf-8 -*-
"""
Utilidades para o exercicio de esquenta (SME)
"""

import re
import os

#def log(message) -> None:
#    """Logs a message"""
#    prefect.context.logger.info(f"\n{message}")

def limpa_numero(numero):
    numero =  (re.sub('[^0-9]+', '', numero))
    return format(int(numero[:-1]), ",").replace(",", "-") + numero[-1]

def cria_caminho(caminho_saida):
    existe = os.path.exists(caminho_saida)
    if not existe:
        os.makedirs(caminho_saida)
      