"""CEC scrape module.

It's a try to make a module for scraping candidates' information
from CEC website on local elections-2015

"""

import os
from bs4 import BeautifulSoup
from collections import OrderedDict
from time import time
import requests
import pandas as pd


START_CANDIDATES_URL = 'https://www.cvk.gov.ua/pls/vm2015/pvm008pt001f01=100pt00_t001f01=100.html'
CURRENT_ELECTION_URL = 'https://www.cvk.gov.ua/pls/vm2015/'
PATH_TO_LOCATION = '2015/local'
OUTPUT_FILENAME = '2015/local/candidates.csv'

CANDIDATE_NAME = 'ПІБ'
CANDIDATE_PARTY = 'Партія'
CANDIDATE_DISTRICT = 'Округ'
CANDIDATE_REGION = 'Область'
CANDIDATE_COUNCIL = 'Рада'
CANDIDATE_INFO = 'Відомості'
CANDIDATE_REGISTRATION_DATE = 'Дата реєстрації'
CANDIDATE_VOTE = '% голосів за'

SELECT_ALL_URLS = 'td > a'
SELECT_COUNCILS = 'td.td2 > b > a'
SELECT_CANDIDATE_NAME = 'table:nth-of-type(5) tr td.td2:nth-of-type(2), table:nth-of-type(5) tr td.td3:nth-of-type(2)'
SELECT_CANDIDATE_PARTY = 'td.td2 > a.a1, td.td3 > a.a1'
CALC_CANDIDATE_PARTY = 'table.t2:nth-of-type(4) td.td2:nth-of-type(2), table.t2:nth-of-type(4) td.td3:nth-of-type(2)'
SELECT_CANDIDATE_DISTRICT = 'table:nth-of-type(5) tr td.td2:first-child, table:nth-of-type(5) tr td.td3:first-child'
SELECT_CANDIDATE_INFO = 'table:nth-of-type(5) tr td.td2small, table:nth-of-type(5) tr td.td3small'
SELECT_CANDIDATE_REGISTRATION_DATE = 'table:nth-of-type(5) tr td.td2:nth-of-type(4),\
                                        table:nth-of-type(5) tr td.td3:nth-of-type(4)'
SELECT_CANDIDATE_VOTE_SELECTOR = 'table:nth-of-type(5) tr td.td2:nth-of-type(5),\
                                    table:nth-of-type(5) tr td.td3:nth-of-type(5)'


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
    Function gets a list with local councils' urls and returns a list with all info about candidates
    :param urls_list: a list of strings
    :return candidates_info: a pandasDataframe (multivector list)
    """
    candidates_info = list()
    for url in urls_list:
        start_time = time()
        candidates_info_dict = OrderedDict()
        response = requests.get(url, verify=False)
        soup = BeautifulSoup(response.content, 'lxml')
        candidates_info_dict[CANDIDATE_NAME] = [soup.select(SELECT_CANDIDATE_NAME)[idx].get_text()
                                                for idx, _ in enumerate(soup.select(SELECT_CANDIDATE_NAME))]
        candidates_info_dict[CANDIDATE_PARTY] = get_filled_party_column(soup)
        candidates_info_dict[CANDIDATE_DISTRICT] = [soup.select(SELECT_CANDIDATE_DISTRICT)[idx].get_text() for idx, _
                                                    in enumerate(soup.select(SELECT_CANDIDATE_DISTRICT))]
        candidates_info_dict[CANDIDATE_REGION] = soup.find('p', {'class': 'p2'}).contents[0]
        candidates_info_dict[CANDIDATE_COUNCIL] = soup.find('p', {'class': 'p2'}).contents[2]
        candidates_info_dict[CANDIDATE_INFO] = [soup.select(SELECT_CANDIDATE_INFO)[idx].get_text() for idx, _ in
                                                enumerate(soup.select(SELECT_CANDIDATE_INFO))]
        candidates_info_dict[CANDIDATE_REGISTRATION_DATE] = [
            soup.select(SELECT_CANDIDATE_REGISTRATION_DATE)[idx].get_text() for idx, _ in
            enumerate(soup.select(SELECT_CANDIDATE_REGISTRATION_DATE))]
        candidates_info_dict[CANDIDATE_VOTE] = [soup.select(SELECT_CANDIDATE_VOTE_SELECTOR)[idx].get_text() for idx, _
                                                in enumerate(soup.select(SELECT_CANDIDATE_VOTE_SELECTOR))]
        candidates_info.append(pd.DataFrame(candidates_info_dict))
        end_time = time()
        print('{0} downloaded in {1:.2f} seconds'.format((candidates_info_dict[CANDIDATE_COUNCIL]),
                                                         end_time - start_time))
    return candidates_info


def get_filled_party_column(soup):
    """
    A service function for filling 'Партія' column with values. Takes party name, number of party candidates and
    multiplies these values for each party on a page
    :param soup: BeautifulSoup object
    :return filled_party_column: a list of strings
    """
    filled_party_column = list()
    party_list = [(soup.select(SELECT_CANDIDATE_PARTY)[idx].get_text(),) for idx, _ in
                  enumerate(soup.select(SELECT_CANDIDATE_PARTY))]
    candidates_number = [soup.select(CALC_CANDIDATE_PARTY)[idx].get_text() for idx, _ in
                         enumerate(soup.select(CALC_CANDIDATE_PARTY))]
    for idx, _ in enumerate(party_list):
        filled_party_column.extend(list((party_list[idx] * int(candidates_number[idx]))))
    return filled_party_column


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
    print('Завантаження успішно завершено {:.2f}.'.format(end_time - start_time))
