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

START_CANDIDATES_URL = 'https://www.cvk.gov.ua/pls/vm2020/pvm002pt001f01=695pt00_t001f01=695.html'
CURRENT_ELECTION_URL = 'https://www.cvk.gov.ua/pls/vm2020/'
PATH_TO_LOCATION = '2020/local'
OUTPUT_FILENAME = '2020/local/elected.csv'

CANDIDATE_NAME = 'ПІБ'
CANDIDATE_PARTY = 'Партія'
CANDIDATE_DISTRICT_LINKED = '№ ТВО за яким закріплено'
CANDIDATE_COUNCIL = 'Область і рада'
CANDIDATE_INFO = 'Відомості про обраного депутата'
CANDIDATE_VOTE = '% голосів за'
CANDIDATE_QUOTA = '% від квоти'
CANDIDATE_DISTRICT_ELECTED = 'Виборчий округ, в якому обрано'

SELECT_ALL_URLS = 'td > a'
SELECT_COUNCILS = 'td.td2 > a.a1'
SELECT_CANDIDATE_NAME = 'table:nth-of-type(6) tr td.td2:nth-of-type(2), table:nth-of-type(6) tr td.td3:nth-of-type(2)'
SELECT_PARTY_FROM_RESULTS_TABLE = 'td.td2 > a.a1, td.td3 > a.a1'
SELECT_PARTY_FROM_CANDIDATE_TABLE = 'table.t2:nth-of-type(6) td.td10'
SELECT_ROWS_IN_CANDIDATE_TABLE = 'table.t2:nth-of-type(6) tr'
SELECT_CANDIDATE_DISTRICT_LINKED = 'table:nth-of-type(6) tr td.td2:nth-of-type(1), table:nth-of-type(6) tr td.td3:nth-of-type(1)'
SELECT_CANDIDATE_INFO = 'table:nth-of-type(6) tr td.td2small, table:nth-of-type(6) tr td.td3small'
SELECT_CANDIDATE_VOTE = 'table:nth-of-type(6) tr td.td2:nth-of-type(4), table:nth-of-type(6) tr td.td3:nth-of-type(4)'
SELECT_CANDIDATE_QUOTA = 'table:nth-of-type(6) tr td.td2:nth-of-type(5), table:nth-of-type(6) tr td.td3:nth-of-type(5)'
SELECT_CANDIDATE_DISTRICT_ELECTED = 'table:nth-of-type(6) tr td.td2:nth-of-type(6), table:nth-of-type(6) tr td.td3:nth-of-type(6)'


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
            candidates_info_dict[CANDIDATE_PARTY] = get_filled_party_column(soup)
            candidates_info_dict[CANDIDATE_COUNCIL] = soup.find('p', {'class': 'p2'}).contents[0]
            candidates_info_dict[CANDIDATE_DISTRICT_LINKED] = [
                soup.select(SELECT_CANDIDATE_DISTRICT_LINKED)[idx].get_text() for
                idx, _ in
                enumerate(soup.select(SELECT_CANDIDATE_DISTRICT_LINKED))]
            candidates_info_dict[CANDIDATE_INFO] = [
                soup.select(SELECT_CANDIDATE_INFO)[idx].get_text() for idx, _ in
                enumerate(soup.select(SELECT_CANDIDATE_INFO))]
            candidates_info_dict[CANDIDATE_VOTE] = [
                soup.select(SELECT_CANDIDATE_VOTE)[idx].get_text() for idx, _ in
                enumerate(soup.select(SELECT_CANDIDATE_VOTE))]
            candidates_info_dict[CANDIDATE_QUOTA] = [soup.select(SELECT_CANDIDATE_QUOTA)[idx].get_text()
                                                     for idx, _ in
                                                     enumerate(soup.select(SELECT_CANDIDATE_QUOTA))]
            candidates_info_dict[CANDIDATE_DISTRICT_ELECTED] = [
                soup.select(SELECT_CANDIDATE_DISTRICT_ELECTED)[idx].get_text() for idx, _ in
                enumerate(soup.select(SELECT_CANDIDATE_DISTRICT_ELECTED))]
            record_intermediate_result(candidates_info_dict)
            candidates_info.append(pd.DataFrame(candidates_info_dict))
            end_time = time()
        except AttributeError:
            continue
        print('{0} downloaded in {1:.2f} seconds'.format((candidates_info_dict[CANDIDATE_COUNCIL]),
                                                         end_time - start_time))
    return candidates_info


def get_filled_party_column(soup):
    """
    A service function for filling 'Партія' column with values. Looks through rows except the first one for party name
    and adds it to a list until finds out the next party name.
    :param soup: BeautifulSoup object
    :return filled_party_column: a list of strings
    """
    filled_party_column = list()
    party = None
    for _, value in enumerate(soup.select(SELECT_ROWS_IN_CANDIDATE_TABLE)):
        if value.select(SELECT_PARTY_FROM_CANDIDATE_TABLE):
            party = value.select(SELECT_PARTY_FROM_CANDIDATE_TABLE)[0].get_text()
            continue
        filled_party_column.append(party)
    return filled_party_column[1:]


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
