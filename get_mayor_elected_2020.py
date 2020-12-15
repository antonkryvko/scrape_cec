#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CEC scrape module.

It's a try to make a module for scraping candidates' information
from CEC website on local elections-2020.

"""

import os
from bs4 import BeautifulSoup
from collections import OrderedDict
from time import time
import requests
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

START_CANDIDATES_URL = 'https://www.cvk.gov.ua/pls/vm2020/pvm003pt001f01=695pt00_t001f01=695.html'
CURRENT_ELECTION_URL = 'https://www.cvk.gov.ua/pls/vm2020/'
PATH_TO_LOCATION = '2020/local'
OUTPUT_FILENAME = '2020/local/mayors_elected.csv'

CANDIDATE_NAME = 'ПІБ'
CANDIDATE_PARTY = 'Партія'
CANDIDATE_REGION = 'Область'
CANDIDATE_COUNCIL = 'Рада'
CANDIDATE_VOTE = 'Кількість голосів за'
CANDIDATE_PERCENT = '% голосів за'

SELECT_ALL_URLS = 'td a.a1small'
SELECT_COUNCILS = 'td > a.a1'
SELECT_CANDIDATE_NAME = 'table.t2 tr td.td2:nth-of-type(1), table.t2 tr td.td3:nth-of-type(1)'
SELECT_CANDIDATE_PARTY = 'table.t2 tr td.td2:nth-of-type(2), table.t2 tr td.td3:nth-of-type(2)'
SELECT_CANDIDATE_VOTE = 'table.t2 tr td.td2:nth-of-type(3), table.t2 tr td.td3:nth-of-type(3)'
SELECT_CANDIDATE_PERCENT = 'table.t2 tr td.td2:nth-of-type(4), table.t2 tr td.td3:nth-of-type(4)'


def get_all_urls():
    start_time = time()
    response = requests.get(START_CANDIDATES_URL, verify=False)
    soup = BeautifulSoup(response.content, 'lxml')
    all_urls = [CURRENT_ELECTION_URL + url['href'] for url in soup.select(SELECT_ALL_URLS)]
    end_time = time()
    print('Urls from start page downloaded in {:.2f} seconds'.format(end_time - start_time))
    return all_urls


def get_urls_to_councils(urls_list):
    start_time = time()
    urls_to_councils = list()
    for url in urls_list:
        response = requests.get(url, verify=False)
        soup = BeautifulSoup(response.content, 'lxml')
        councils = [CURRENT_ELECTION_URL + url['href'] for url in soup.select(SELECT_COUNCILS)]
        urls_to_councils.extend(councils)
    end_time = time()
    print('Urls from regional pages downloaded in {:.2f} seconds'.format(end_time - start_time))
    return urls_to_councils


def get_candidates_info(urls_list):
    """
    Function gets a list with local councils' urls and returns a list with all info about candidates.
    For preventing data loss, function writes intermediate results to appropriate files.
    :param urls_list: a list of strings
    :return candidates_info: a pandasDataframe (multivector list)
    """
    candidates_info = list()
    for url in urls_list:
        try:
            start_time = time()
            candidates_info_dict = OrderedDict()
            response = requests.get(url, verify=False)
            soup = BeautifulSoup(response.content, 'lxml')
            candidates_info_dict[CANDIDATE_NAME] = [soup.select(SELECT_CANDIDATE_NAME)[idx].get_text()
                                                    for idx, _ in enumerate(soup.select(SELECT_CANDIDATE_NAME))]
            candidates_info_dict[CANDIDATE_PARTY] = [soup.select(SELECT_CANDIDATE_PARTY)[idx].get_text()
                                                     for idx, _ in
                                                     enumerate(soup.select(SELECT_CANDIDATE_PARTY))]
            candidates_info_dict[CANDIDATE_REGION] = soup.find('p', {'class': 'p1'}).findNext('p').contents[1]
            candidates_info_dict[CANDIDATE_COUNCIL] = soup.find('p', {'class': 'p2'}).contents[0]
            candidates_info_dict[CANDIDATE_VOTE] = [
                soup.select(SELECT_CANDIDATE_VOTE)[idx].get_text() for idx, _ in
                enumerate(soup.select(SELECT_CANDIDATE_VOTE))]
            candidates_info_dict[CANDIDATE_PERCENT] = [soup.select(SELECT_CANDIDATE_PERCENT)[idx].get_text()
                                                       for idx, _ in
                                                       enumerate(soup.select(SELECT_CANDIDATE_PERCENT))]
            record_intermediate_result(candidates_info_dict)
            candidates_info.append(pd.DataFrame(candidates_info_dict))
            end_time = time()
        except AttributeError:
            continue
        print('{0} downloaded in {1:.2f} seconds'.format((candidates_info_dict[CANDIDATE_COUNCIL]),
                                                         end_time - start_time))
    return candidates_info


def record_intermediate_result(candidates_info_dict):
    path_to_directory = PATH_TO_LOCATION + '/{}/'.format(candidates_info_dict[CANDIDATE_COUNCIL])
    if not os.path.exists(path_to_directory):
        os.makedirs(path_to_directory)
    pd.DataFrame(candidates_info_dict).to_csv(path_to_directory + candidates_info_dict[CANDIDATE_COUNCIL] + '.csv')


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
    print('Починаю завантаження списків обраних депутатів')
    main()
    end_time = time()
    print('Завантаження успішно завершено за {:.2f} секунди.'.format(end_time - start_time))
