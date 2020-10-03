#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from bs4 import BeautifulSoup
from collections import OrderedDict
from time import time
import requests
import pandas as pd
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

START_CANDIDATES_URL = 'https://www.cvk.gov.ua/pls/vm2010/wm00114pt00_t001f01=800pxto=0.html'
CURRENT_ELECTION_URL = 'https://www.cvk.gov.ua/pls/vm2010/'
PATH_TO_LOCATION = '2010/local'
OUTPUT_FILENAME = '2010/local/elected.csv'

CANDIDATE_NAME = 'ПІБ'
CANDIDATE_PARTY = 'Партія'
CANDIDATE_DISTRICT = 'Округ'
CANDIDATE_REGION = 'Область'
CANDIDATE_COUNCIL = 'Рада'
CANDIDATE_INFO = 'Відомості'
CANDIDATE_ELECTION_DATE = 'Дата реєстрації'

SELECT_ALL_URLS = 'table.t2 td > a'
SELECT_COUNCILS = 'a.a1small'
SELECT_CANDIDATE_NAME = 'table:nth-of-type(5) tr td.td2small:nth-of-type(3),table:nth-of-type(5) tr ' \
                        'td.td3small:nth-of-type(3) '
SELECT_ROWS_IN_CANDIDATE_TABLE = 'table.t2:nth-of-type(5) tr'
SELECT_PARTY_FROM_CANDIDATE_TABLE = 'table.t2:nth-of-type(5) td.td10'
SELECT_PARTY_FROM_RESULTS_TABLE = 'table.t2:nth-of-type(4) td.td2small:first-child'
CALC_CANDIDATE_PARTY = 'table.t2:nth-of-type(4) td.td2small:nth-of-type(2), table.t2:nth-of-type(4) ' \
                       'td.td3small:nth-of-type(2) '
SELECT_CANDIDATE_DISTRICT = 'table:nth-of-type(5) tr td.td2small:nth-of-type(2),table:nth-of-type(5) tr ' \
                            'td.td3small:nth-of-type(2) '
SELECT_CANDIDATE_INFO = 'table:nth-of-type(5) tr td.td2small:nth-of-type(5),table:nth-of-type(5) tr ' \
                        'td.td3small:nth-of-type(5) '
SELECT_CANDIDATE_ELECTION_DATE = 'table:nth-of-type(5) tr td.td2small:nth-of-type(4),table:nth-of-type(5) tr ' \
                                 'td.td3small:nth-of-type(4) '


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
            candidates_info_dict[CANDIDATE_PARTY] = get_filled_party_column(
                soup) if None not in get_filled_party_column(soup) else soup.select(SELECT_PARTY_FROM_RESULTS_TABLE)[
                0].get_text()
            candidates_info_dict[CANDIDATE_DISTRICT] = [soup.select(SELECT_CANDIDATE_DISTRICT)[idx].get_text() for
                                                        idx, _
                                                        in enumerate(soup.select(SELECT_CANDIDATE_DISTRICT))]
            candidates_info_dict[CANDIDATE_REGION] = soup.find('p', {'class': 'p1'}).contents[1]
            candidates_info_dict[CANDIDATE_COUNCIL] = soup.find('p', {'class': 'p2'}).contents[0]
            candidates_info_dict[CANDIDATE_INFO] = [soup.select(SELECT_CANDIDATE_INFO)[idx].get_text() for idx, _ in
                                                    enumerate(soup.select(SELECT_CANDIDATE_INFO))]
            candidates_info_dict[CANDIDATE_ELECTION_DATE] = [
                soup.select(SELECT_CANDIDATE_ELECTION_DATE)[idx].get_text() for idx, _ in
                enumerate(soup.select(SELECT_CANDIDATE_ELECTION_DATE))]
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
    path_to_directory = PATH_TO_LOCATION + '/{}/'.format(candidates_info_dict[CANDIDATE_REGION])
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
    print('Починаю завантаження списків кандидатів')
    main()
    end_time = time()
    print('Завантаження успішно завершено за {:.2f} секунди.'.format(end_time - start_time))
