#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CEC scrape module.

It's a try to make a module for scraping candidates' information
from CEC website on any elections you want

"""

import os
from bs4 import BeautifulSoup
from collections import OrderedDict
from time import time
import requests
import pandas as pd


START_URL = 'https://www.cvk.gov.ua/pls/vnd2019/wp032pt001f01=919.html'
CURRENT_ELECTION_URL = 'https://www.cvk.gov.ua/pls/vnd2019/'
PATH_TO_LOCATION_WITH_PHOTOS = '2019/constituencies/photos/'
PATH_TO_LOCATION_WITH_PROGRAMS = '2019/constituencies/programs/'
SELECT_DISTRICTS = 'td.cntr:nth-of-type(2) > a.a2'
SELECT_CANDIDATES_ON_DISTRICTS = 'td.cntr:nth-of-type(1) > a.a2'
SELECT_CANDIDATE_FULLNAME = 'h1'
SELECT_CANDIDATE_INFO_FROM_TABLE = 'table tr > td:nth-of-type(2)'
SELECT_ELECTION_PROGRAM = 'table tr > td:nth-of-type(2) > a.a1'
SELECT_TRUSTEES_INFO = 'table tr > td:nth-of-type(2) > a.a2'
NUMBER_OF_ROWS_IN_TABLE = 7
START_DISTRICT = 11
OUTPUT_FILENAME = '2019/constituencies/2019_constituencies_candidates.csv'

TITLE_FULLNAME = 'ПІБ'
TITLE_DISTRICT = 'Округ'
TITLE_NOMINATION = 'Висування'
TITLE_REGISTRATION = 'Номер і дата реєстрації'
TITLE_CANCELLATION = 'Номер та дата рішення про скасування реєстрації кандидата'
TITLE_INFO = 'Основні відомості'
TITLE_PHOTO = 'Фотографія'
TITLE_PROGRAM = 'Передвиборна програма'
TITLE_TRUSTEES = 'Довірені особи'


def get_links_to_districts():
    start_time = time()
    response = requests.get(START_URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.select(SELECT_DISTRICTS)
    districts = [CURRENT_ELECTION_URL + links[idx]['href'] for idx, link in (enumerate(links))]
    end_time = time()
    print('Посилання на сторінки округів завантажені за {:.2f}'.format(end_time - start_time))
    return districts


def get_links_to_candidates_on_districts(district_counter, district):
    start_time = time()
    response = requests.get(district)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.select(SELECT_CANDIDATES_ON_DISTRICTS)
    candidates_on_districts = [CURRENT_ELECTION_URL + links[idx]['href'] for idx, link in (enumerate(links))]
    end_time = time()
    print('Завантажені посилання на профілі кандидатів на окрузі №{0} за {1:.2f} секунд'.format(district_counter,
                                                                                                end_time - start_time))
    return candidates_on_districts


def get_candidate_info(candidate_url):
    """Function gets an ordered dictionary with a candidate's information.

    This is: full name, district, nomination subject, registration status,
    background, trusted persons and links to program and photo. There are some
    differences in markdown whether is there registration cancellation or not,
    which are hold by if-statement and try-except block.

    Parameters
    ----------
    candidate_url: string
        a link to the candidate's page

    Returns
    -------
    candidate_info_dict: OrderedDict
        a dictionary with candidate's page column contents,
        a link to candidate's photo and a link to candidate's program

    """
    start_time = time()
    response = requests.get(candidate_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    candidate_info_dict = OrderedDict()
    candidate_info_dict[TITLE_FULLNAME] = soup.select(SELECT_CANDIDATE_FULLNAME)[0].get_text()
    candidate_info_dict[TITLE_DISTRICT] = soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[0].get_text()
    candidate_info_dict[TITLE_NOMINATION] = soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[1].get_text()
    candidate_info_dict[TITLE_REGISTRATION] = soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[3].get_text()
    if len(soup.find_all('tr')) <= NUMBER_OF_ROWS_IN_TABLE:
        candidate_info_dict[TITLE_CANCELLATION] = ''
        candidate_info_dict[TITLE_INFO] = soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[4].get_text()
    else:
        candidate_info_dict[TITLE_CANCELLATION] = soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[4].get_text()
        candidate_info_dict[TITLE_INFO] = soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[6].get_text()
    candidate_info_dict[TITLE_PROGRAM] = "{0}{1}".format(CURRENT_ELECTION_URL,
                                                         str(soup.select(SELECT_ELECTION_PROGRAM)[0].get('href')))
    candidate_info_dict[TITLE_PHOTO] = CURRENT_ELECTION_URL + str(soup.img['src'])
    try:
        trustees_link = "{0}{1}".format(CURRENT_ELECTION_URL, str(soup.select(SELECT_TRUSTEES_INFO)[0].get('href')))
    except IndexError:
        candidate_info_dict[TITLE_TRUSTEES] = ''
    else:
        candidate_info_dict[TITLE_TRUSTEES] = get_candidate_trustees(trustees_link)
    end_time = time()
    print('Завантажена інформація про {0} за {1:.2f} секунд'.format(candidate_info_dict[TITLE_FULLNAME],
                                                                    end_time - start_time))
    return candidate_info_dict


def get_candidate_trustees(trustees_link):
    response = requests.get(trustees_link)
    soup = BeautifulSoup(response.content, 'html.parser')
    trustees = ','.join(trustee.get_text() for trustee in soup.find_all('td', class_='b'))
    return trustees


def download_candidate_photo(photo_url, candidate_info_dict):
    response = requests.get(photo_url)
    photo_name = candidate_info_dict[TITLE_FULLNAME].replace(' ', '_') + '.' + photo_url.split('.')[-1]
    if not os.path.exists(PATH_TO_LOCATION_WITH_PHOTOS):
        os.makedirs(PATH_TO_LOCATION_WITH_PHOTOS)
    with open((PATH_TO_LOCATION_WITH_PHOTOS + photo_name), 'wb') as f:
        f.write(response.content)
    print('Завантажена фотографія {}'.format(candidate_info_dict[TITLE_FULLNAME]))


def download_candidate_program(program_url, candidate_info_dict):
    response = requests.get(program_url)
    program_name = candidate_info_dict[TITLE_FULLNAME].replace(' ', '_') + '.' + program_url.split('.')[-1]
    if not os.path.exists(PATH_TO_LOCATION_WITH_PROGRAMS):
        os.makedirs(PATH_TO_LOCATION_WITH_PROGRAMS)
    try:
        with open((PATH_TO_LOCATION_WITH_PROGRAMS + program_name), 'wb') as f:
            f.write(response.content)
    except FileNotFoundError:
        return
    print('Завантажена програма {}'.format(candidate_info_dict[TITLE_FULLNAME]))


def get_csv_with_candidates_info(candidates_info_dicts):
    cands_info = pd.concat(candidates_info_dicts, ignore_index=True)
    cands_info.index.name = 'id'
    cands_info.to_csv(OUTPUT_FILENAME)


def main():
    candidates_info_dicts = []
    districts = get_links_to_districts()
    for district_counter, district in enumerate(districts, start=START_DISTRICT):
        candidates_on_districts = get_links_to_candidates_on_districts(district_counter, district)
        for candidate in candidates_on_districts:
            candidate_info_dict = get_candidate_info(candidate)
            download_candidate_photo(candidate_info_dict[TITLE_PHOTO], candidate_info_dict)
            download_candidate_program(candidate_info_dict[TITLE_PROGRAM], candidate_info_dict)
            candidates_info_dicts.append(pd.DataFrame(candidate_info_dict, index=[0]))
    get_csv_with_candidates_info(candidates_info_dicts)


if __name__ == '__main__':
    start_time = time()
    print('Починаю завантаження списків кандидатів')
    main()
    end_time = time()
    print('Завантаження успішно завершено за {:.2f} секунди.'.format(end_time - start_time))
