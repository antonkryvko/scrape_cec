#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CEC scrape module.

A script to find cases when a person candidates to a local councils and a mayor at the same time.
"""

import pandas as pd
from collections import OrderedDict
from time import time

FILE_WITH_CANDIDATES = '2020/local/candidates_with_birthdates.csv'
FILE_WITH_MAYORS = '2020/local/mayors.csv'

FULLNAME = 'ПІБ'
DEPUTY_BIRTHDATE = 'Дата народження депутата'
MAYOR_BIRTHDATE = 'Дата народження мера'
COUNT = 'Кількість балотувань'
DEPUTY_COUNCIL = 'У депутати'
DEPUTY_REGION = 'Область у депутати'
DEPUTY_NOMINEE = 'Висування у депутати'
MAYOR_COUNCIL = 'У міські голови'
MAYOR_REGION = 'Область у голови'
MAYOR_NOMINEE = 'Висування у голови'
FIRST_CANDIDATE = 'Перший кандидат'


def prepare_dataframes(first_file, second_file):
    candidates = pd.read_csv(first_file)
    mayors = pd.read_csv(second_file)
    return candidates, mayors


def analyze_candidates_dataframes(candidates, mayors):
    temp = list()
    for name in candidates['ПІБ'].unique():
        start_time = time()
        datadict = OrderedDict()
        datadict[FULLNAME] = name
        datadict[DEPUTY_BIRTHDATE] = candidates['Дата народження'].loc[candidates['ПІБ'] == name]
        datadict[MAYOR_BIRTHDATE] = mayors['Дата народження'].loc[mayors['ПІБ'] == name]
        datadict[COUNT] = candidates['Рада'].loc[candidates['ПІБ'] == name].count() + mayors['Рада'].loc[
            mayors['ПІБ'] == name].count()
        if datadict[COUNT] == 1:
            continue
        datadict[DEPUTY_COUNCIL] = candidates['Рада'].loc[candidates['ПІБ'] == name]
        datadict[DEPUTY_REGION] = candidates['Область'].loc[candidates['ПІБ'] == name]
        datadict[DEPUTY_NOMINEE] = candidates['Партія'].loc[candidates['ПІБ'] == name]
        if 'Перший кандидат' in candidates['Округ'].loc[candidates['ПІБ'] == name]:
            datadict[FIRST_CANDIDATE] = 'ТАК'
        else:
            datadict[FIRST_CANDIDATE] = 'НІ'
        datadict[MAYOR_COUNCIL] = mayors['Рада'].loc[mayors['ПІБ'] == name]
        datadict[MAYOR_REGION] = mayors['Область'].loc[mayors['ПІБ'] == name]
        datadict[MAYOR_NOMINEE] = mayors['Субʼєкт висування'].loc[mayors['ПІБ'] == name]
        temp.append(pd.DataFrame(datadict, columns=datadict.keys()))
        end_time = time()
        print('{0} downloaded in {1:.2f} seconds'.format(datadict[FULLNAME], end_time - start_time))
        return temp


candidates_dataframe, mayors_dataframe = prepare_dataframes(FILE_WITH_CANDIDATES, FILE_WITH_MAYORS)
temp_dataframe = analyze_candidates_dataframes(candidates_dataframe, mayors_dataframe)
pd.concat(temp_dataframe, ignore_index=True).to_csv('output.csv')
print('Successfully finished')
