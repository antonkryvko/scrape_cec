"""CEC scrape module.

It's a try to make a module for scraping candidates' information
from CEC website on any elections you want

"""

import os
from bs4 import BeautifulSoup
from collections import OrderedDict
import requests
import pandas as pd


START_URL = 'https://www.cvk.gov.ua/pls/vnd2019/wp400pt001f01=919.html'
CURRENT_ELECTION_URL = 'https://www.cvk.gov.ua/pls/vnd2019/'
SELECT_PARTY_LISTS = 'td.cntr:nth-of-type(4) > a.a2'
SELECT_PARTY_PROGRAMS = 'td.cntr:nth-of-type(5) > a.a2'
SELECT_PARTY_CANDIDATES = 'td:nth-of-type(2) > a.a2'
PATH_TO_LOCATION_WITH_PROGRAMS = '2019/parties/programs/'
PATH_TO_LOCATION_WITH_PHOTOS = '2019/parties/photos/'
SELECT_CANDIDATE_FULLNAME = 'h1'
SELECT_CANDIDATE_INFO_FROM_TABLE = 'table tr > td:nth-of-type(2)'
SELECT_ELECTION_PROGRAM = 'table tr > td:nth-of-type(2) > a.a1'
SELECT_TRUSTEES_INFO = 'table tr > td:nth-of-type(2) > a.a2'
NUMBER_OF_ROWS_IN_TABLE = 5
OUTPUT_FILENAME = '2019/parties/2019_party_candidates.csv'

TITLE_PARTY = 'Партія'
TITLE_NUMBER = 'Номер у списку'
TITLE_FULLNAME = 'ПІБ'
TITLE_REGISTRATON = 'Номер і дата реєстрації'
TITLE_CANCELLATION = 'Номер та дата рішення про скасування реєстрації кандидата'
TITLE_INFO = 'Основні відомості'
TITLE_PHOTO = 'Фотографія'


def get_links_to_party_lists(START_URL):
    request = requests.get(START_URL)
    soup = BeautifulSoup(request.content, 'html.parser')
    links = soup.select(SELECT_PARTY_LISTS)
    party_lists = [CURRENT_ELECTION_URL + links[idx]['href']
                   for idx, link in (enumerate(links))]
    print('Посилання на сторінки із списками партій завантажені')
    return party_lists


def get_links_to_party_candidates(party_counter, party_list):
    request = requests.get(party_list)
    soup = BeautifulSoup(request.content, 'html.parser')
    links = soup.select(SELECT_PARTY_CANDIDATES)
    party_candidates = [CURRENT_ELECTION_URL + links[idx]['href']
                        for idx, link in (enumerate(links))]
    print(
        'Завантажені посилання на профілі кандидатів для партії №{}'.format(
            party_counter))
    return party_candidates


def get_party_candidate_info(party_candidate_url):
    """Function gets an ordered dictionary with a party candidate's information.

    This is: full name, party, registration status, background and links to
    photo. There are some differences in markdown whether there is registration
    cancellation or not, which are hold by if-statement and try-except block.

    Parameters
    ----------
    party_candidate_url: string
        a link to the party_candidate's page

    Returns
    -------
    party_candidate_info_dict: OrderedDict
        a dictionary with party_candidate's page column contents,
        a link to candidate's photo and a link to party_candidate's program

    """

    request = requests.get(party_candidate_url)
    soup = BeautifulSoup(request.content, 'html.parser')
    party_candidate_info_dict = OrderedDict()
    party_candidate_info_dict[TITLE_PARTY] = \
        soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[0].get_text()
    party_candidate_info_dict[TITLE_NUMBER] = \
        soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[1].get_text()
    party_candidate_info_dict[TITLE_FULLNAME] = \
        soup.select(SELECT_CANDIDATE_FULLNAME)[0].get_text()
    party_candidate_info_dict[TITLE_REGISTRATON] = \
        soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[2].get_text()
    party_candidate_info_dict[TITLE_INFO] = \
        soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[4].get_text()
    if len(soup.find_all('tr')) <= NUMBER_OF_ROWS_IN_TABLE:
        party_candidate_info_dict[TITLE_CANCELLATION] = ''
        party_candidate_info_dict[TITLE_INFO] = \
            soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[3].get_text()
    else:
        party_candidate_info_dict[TITLE_CANCELLATION] = \
            soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[3].get_text()
        party_candidate_info_dict[TITLE_INFO] = \
            soup.select(SELECT_CANDIDATE_INFO_FROM_TABLE)[5].get_text()
    party_candidate_info_dict[TITLE_PHOTO] = CURRENT_ELECTION_URL + \
        str(soup.img['src'])
    print('Завантажена інформація про {}'.format(
        party_candidate_info_dict[TITLE_FULLNAME]))
    return party_candidate_info_dict


def download_party_candidate_photo(photo_url, party_candidate_info_dict):
    response = requests.get(photo_url)
    photo_name = party_candidate_info_dict[TITLE_FULLNAME].replace(' ', '_') + \
        '.' + photo_url.split('.')[-1]
    if not os.path.exists(PATH_TO_LOCATION_WITH_PHOTOS):
        os.makedirs(PATH_TO_LOCATION_WITH_PHOTOS)
    with open((PATH_TO_LOCATION_WITH_PHOTOS + photo_name), 'wb') as f:
        f.write(response.content)
    print('Завантажена фотографія {}'.format(
        party_candidate_info_dict[TITLE_FULLNAME]))


def get_party_programs():
    request = requests.get(START_URL)
    soup = BeautifulSoup(request.content, 'html.parser')
    links = soup.select(SELECT_PARTY_PROGRAMS)
    party_programs = [CURRENT_ELECTION_URL + links[idx]['href']
                      for idx, link in (enumerate(links))]
    print('Посилання на програму партії')
    return party_programs


def download_party_programs(program_url, party_candidates_info_dicts):
    response = requests.get(program_url)
    program_name = party_candidates_info_dicts[TITLE_PARTY].replace(' ', '_') + \
        '.' + program_url.split('.')[-1]
    if not os.path.exists(PATH_TO_LOCATION_WITH_PROGRAMS):
        os.makedirs(PATH_TO_LOCATION_WITH_PROGRAMS)
    try:
        with open((PATH_TO_LOCATION_WITH_PROGRAMS + program_name), 'wb') as f:
            f.write(response.content)
    except FileNotFoundError:
        return
    print('Завантажена програма {}'.format(
        party_candidates_info_dicts[TITLE_PARTY]))


def get_csv_with_party_candidates_info(party_candidates_info_dicts):
    party_cands_info = pd.concat(party_candidates_info_dicts, ignore_index=True)
    party_cands_info.index.name = 'id'
    party_cands_info.to_csv(OUTPUT_FILENAME)


def main():
    party_candidates_info_dicts = []
    party_lists = get_links_to_party_lists(START_URL)
    for party_counter, party_list in enumerate(party_lists, start=1):
        party_candidates = \
            get_links_to_party_candidates(party_counter, party_list)
        for party_candidate in party_candidates:
            party_candidate_info_dict = \
                get_party_candidate_info(party_candidate)
            download_party_candidate_photo(
                party_candidate_info_dict[TITLE_PHOTO],
                party_candidate_info_dict)
            party_candidates_info_dicts.append(pd.DataFrame(
                party_candidate_info_dict, index=[0]))
    get_csv_with_party_candidates_info(party_candidates_info_dicts)
#           get_party_programs(START_URL) разобраться какой список используется
#           download_party_programs(party_programs, party_candidates_info_dicts)


if __name__ == '__main__':
    print('Починаю завантаження')
    try:
        main()
    except:
        print('Помилка')
    else:
        print('Завантаження успішне')
