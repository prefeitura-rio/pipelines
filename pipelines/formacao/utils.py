# -*- coding: utf-8 -*-
"""
Utils for formacao
"""
# pylint: disable=W0702, W0640, W0102
import prefect
import phonenumbers as pnum

def log(msg) -> None:
    """Logs a message"""
    prefect.context.logger.info(f"\n{msg}")

def format_number(number,nat):
    '''Funcao que recebe um numero telefone e nacionalidade e formata para o
    formato internacional. Numeros do Canada e Nova Zelandia nao foram identificados.'''
    
    formato = pnum.PhoneNumberFormat.INTERNATIONAL
   
    try:
        yyy = pnum.parse(number,nat)
    except:
        #print('Formato de numero nao encontrado: {}. País: {}'.format(number,nat))
        return number
    
    y_format = pnum.format_number(yyy,formato)
    
    return y_format

def df_formatnum(dframe,colunas=["phone","cell"], nat="nat"):
    '''Funcao que recebe um dataframe, colunas com os numeros e coluna com
    nacionalidade e retorna um dataframe com numeros em formato internacional.'''
    
    for col in colunas:
        dframe[col] = dframe.apply(lambda x: format_number(x[col],x[nat]), axis=1)
    
    return dframe
  