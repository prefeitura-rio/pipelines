# -*- coding: utf-8 -*-

# Funções de apoio

# ***************************************************************************************
def FormataTexto(texto, sep="-", pos=3):
    """O que a função faz:
    Formata um texto usando o separador sep na posição pos

    Argumentos:
        texto: texto a ser formatado
        sep: é o tipo de separador a ser usado na formatação. Padrão: '-'
        pos: é a posição em que entrará o separador a partir do final. Padrão: 3

    Retorna:
        o texto formatado

    Obs:a formatação se dará do final para o início, se repetindo até o início
    Exemplo: print(FormataTexto('1234567890', '-', 3)) => retorna: 1-234-567-890"""

    new = ""
    tam = len(texto)
    for i in range(tam):
        resto = (tam - i) % pos
        if (i != 0) and (resto == 0):
            new = new + sep + texto[i]
        else:
            new = new + texto[i]
    return new
