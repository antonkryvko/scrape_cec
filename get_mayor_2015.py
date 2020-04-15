#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CEC scrape module.

It's a try to make a module for scraping mayors' information
from CEC website on local elections-2015

"""

import os
from bs4 import BeautifulSoup
from collections import OrderedDict
from time import time
import requests
import pandas as pd

START_MAYORS_URL = 'https://www.cvk.gov.ua/pls/vm2015/pvm117pt001f01=100.html'
CURRENT_ELECTION_URL = 'https://www.cvk.gov.ua/pls/vm2015/'
PATH_TO_LOCATION = '2015/local'
OUTPUT_FILENAME = '2015/local/mayors.csv'

CANDIDATE_NAME = 'ПІБ'
CANDIDATE_BIRTHDAY = 'Дата народження'
CANDIDATE_PARTY = 'Субʼєкт висування'
CANDIDATE_COUNCIL = 'Рада'
CANDIDATE_REGION = 'Область'
CANDIDATE_INFO = 'Відомості'
CANDIDATE_REGISTRATION_DATE = 'Дата реєстрації'

SELECT_REGION_URLS = 'td > a'
SELECT_COUNCILS = 'td.td3small > a.a1small'
SELECT_CANDIDATE_NAME = 'table:nth-of-type(4) td.td2, table:nth-of-type(4) td.td3'
SELECT_CANDIDATE_BIRTHDAY = 'td.td2small:nth-child(2), td.td3small:nth-child(2)'
SELECT_CANDIDATE_PARTY = 'td.td2small:nth-child(3), td.td3small:nth-child(3)'
SELECT_CANDIDATE_INFO = 'td.td2small:nth-child(4), td.td3small:nth-child(4)'
SELECT_CANDIDATE_REGISTRATION_DATE = 'td.td2small:nth-child(5), td.td3small:nth-child(5)'
SELECT_CANDIDATE_COUNCIL = 'p.p2'
SELECT_CANDIDATE_REGION = 'p.p1:has(img)'


def get_all_urls():
    start_time = time()
    response = requests.get(START_MAYORS_URL, verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    all_urls = [CURRENT_ELECTION_URL + url['href'] for url in soup.select(SELECT_REGION_URLS)]
    end_time = time()
    print('Urls from start page downloaded in {:.2f} seconds'.format(end_time - start_time))
    return all_urls


def get_urls_to_councils(urls_list):
    start_time = time()
    urls_to_councils = list()
    for url in urls_list:
        response = requests.get(url, verify=False)
        soup = BeautifulSoup(response.content, 'html.parser')
        councils = [CURRENT_ELECTION_URL + url['href'] for url in soup.select(SELECT_COUNCILS)]
        urls_to_councils.extend(councils)
    end_time = time()
    print('Urls from regional pages downloaded in {:.2f} seconds'.format(end_time - start_time))
    return urls_to_councils


def get_candidates_info(urls_list):
    candidates_info = list()
    for url in urls_list:
        start_time = time()
        candidates_info_dict = OrderedDict()
        response = requests.get(url, verify=False)
        soup = BeautifulSoup(response.content, 'lxml')
        candidates_info_dict[CANDIDATE_NAME] = [soup.select(SELECT_CANDIDATE_NAME)[idx].get_text()
                                                for idx, _ in enumerate(soup.select(SELECT_CANDIDATE_NAME))]
        candidates_info_dict[CANDIDATE_BIRTHDAY] = [soup.select(SELECT_CANDIDATE_BIRTHDAY)[idx].get_text() for idx, _
                                                    in enumerate(soup.select(SELECT_CANDIDATE_BIRTHDAY))]
        candidates_info_dict[CANDIDATE_PARTY] = [soup.select(SELECT_CANDIDATE_PARTY)[idx].get_text().strip() for idx, _
                                                 in enumerate(soup.select(SELECT_CANDIDATE_PARTY))]
        candidates_info_dict[CANDIDATE_REGION] = soup.select(SELECT_CANDIDATE_REGION)[0].get_text().strip()
        candidates_info_dict[CANDIDATE_COUNCIL] = soup.select(SELECT_CANDIDATE_COUNCIL)[0].get_text()
        candidates_info_dict[CANDIDATE_INFO] = [soup.select(SELECT_CANDIDATE_INFO)[idx].get_text() for idx, _ in
                                                enumerate(soup.select(SELECT_CANDIDATE_INFO))]
        candidates_info_dict[CANDIDATE_REGISTRATION_DATE] = [
            soup.select(SELECT_CANDIDATE_REGISTRATION_DATE)[idx].get_text() for idx, _ in
            enumerate(soup.select(SELECT_CANDIDATE_REGISTRATION_DATE))]
        candidates_info.append(pd.DataFrame(candidates_info_dict))
        end_time = time()
        print('{0} downloaded in {1:.2f} seconds'.format((candidates_info_dict[CANDIDATE_COUNCIL]),
                                                         end_time - start_time))
    return candidates_info


def get_csv_with_candidates_info(candidates_info_list):
    candidates_output = pd.concat(candidates_info_list, ignore_index=True)
    candidates_output.index.name = 'id'
    if not os.path.exists(PATH_TO_LOCATION):
        os.makedirs(PATH_TO_LOCATION)
    print('Записую результати у файл')
    candidates_output.to_csv(OUTPUT_FILENAME)


def main():
    try:
        all_urls = get_all_urls()
        urls_to_councils = get_urls_to_councils(all_urls)
        candidates_info = get_candidates_info(urls_to_councils)
        get_csv_with_candidates_info(candidates_info)
    except ConnectionError:
        print('Завантаження перервано через проблеми з мережею')


if __name__ == '__main__':
    start_time = time()
    print('Починаю завантаження списків кандидатів')
    main()
    end_time = time()
    print('Завантаження успішно завершено за {:.2f} секунди.'.format(end_time - start_time))
