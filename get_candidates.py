"""CEC scrape module.

It's a try to make a module for scraping candidates' information
from CEC website on any elections you want

"""

import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from collections import OrderedDict

START_URL = 'https://www.cvk.gov.ua/pls/vnd2019/wp032pt001f01=919.html'
CURRENT_ELECTION_URL = 'https://www.cvk.gov.ua/pls/vnd2019/'
PATH_TO_LOCATION_WITH_PHOTOS = 'photos/'
PATH_TO_LOCATION_WITH_PROGRAMS = 'programs/'


def get_links_to_districts():
    """Function gets links to districts' pages from the election's page.

    Returns
    -------
    list
        a list of links to districts' pages from the election's page

    """

    request = requests.get(START_URL)
    soup = BeautifulSoup(request.content, 'html.parser')
    links = soup.select('td.cntr:nth-of-type(2) > a.a2')
    districts = [CURRENT_ELECTION_URL + links[idx]['href']
                 for idx, link in (enumerate(links))]
    print('Посилання на сторінки округів завантажені')
    return districts


def get_links_to_candidates_on_districts(i, district_url):
    """Function gets links to candidates' pages from the district's page.

    Parameters
    ----------
    i: integer
        an iterator, which counts numbers of districts
    district_url: string
        a link to district's page

    Returns
    -------
    list
        a list of links to candidates' pages from the district's page

    """

    request = requests.get(district_url)
    soup = BeautifulSoup(request.content, 'html.parser')
    links = soup.select('td.cntr:nth-of-type(1) > a.a2')
    candidates_on_districts = [CURRENT_ELECTION_URL + links[idx]['href']
                               for idx, link in (enumerate(links))]
    print(
        'Завантажені посилання на профілі кандидатів на окрузі №{}'.format(i))
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

    request = requests.get(candidate_url)
    soup = BeautifulSoup(request.content, 'html.parser')
    candidate_info_dict = OrderedDict()
    candidate_info_dict['ПІБ'] = soup.h1.get_text()
    candidate_info_dict['Округ'] = \
        soup.select('table tr > td:nth-of-type(2)')[0].get_text()
    candidate_info_dict['Висування'] = \
        soup.select('table tr > td:nth-of-type(2)')[1].get_text()
    candidate_info_dict['Номер і дата реєстрації'] = \
        soup.select('table tr > td:nth-of-type(2)')[3].get_text()
    if len(soup.find_all('tr')) <= 7:
        candidate_info_dict[
            'Номер та дата рішення про скасування реєстрації кандидата'
            ] = ''
        candidate_info_dict['Основні відомості'] = \
            soup.select('table tr > td:nth-of-type(2)')[4].get_text()
    else:
        candidate_info_dict[
            'Номер та дата рішення про скасування реєстрації кандидата'
            ] = soup.select('table tr > td:nth-of-type(2)')[4].get_text()
        candidate_info_dict['Основні відомості'] = \
            soup.select('table tr > td:nth-of-type(2)')[6].get_text()
    candidate_info_dict['Передвиборна програма'] = CURRENT_ELECTION_URL + \
        str(soup.select('table tr > td:nth-of-type(2) > a.a1')[0].get('href'))
    candidate_info_dict['Фотографія'] = CURRENT_ELECTION_URL + \
        str(soup.img['src'])
    try:
        trustees_link = CURRENT_ELECTION_URL + \
            str(soup.select(
                'table tr > td:nth-of-type(2) > a.a2'
                )[0].get('href'))
    except IndexError:
        candidate_info_dict['Довірені особи'] = ''
    else:
        candidate_info_dict['Довірені особи'] = \
            get_candidate_trustees(trustees_link)
    print('Завантажена інформація про {}'.format(candidate_info_dict['ПІБ']))
    return candidate_info_dict


def get_candidate_trustees(trustees_link):
    """Function gets a string with candidate's trusted persons.

    Parameters
    ----------
    trustees_link: string
        a link to candidate's trusted persons page

    Returns
    -------
    trustees: string
        a string with candidate's trusted persons

    """

    request = requests.get(trustees_link)
    soup = BeautifulSoup(request.content, 'html.parser')
    trustees = ','.join(trustee.get_text() for trustee in soup.find_all(
        'td', class_='b'))
    return trustees


def download_candidate_photo(photo_url, candidate_info_dict):
    """Function downloads candidate's photo.

    In case there is no catalogue to save photo into, creates one. The name of
    photo is candidate's name plus file extension.

    Parameters
    ----------
    photo_url: string
        a link to candidate's photo, received from candidate_info_dict
    candidate_info_dict: OrderedDict
        a dictionary with candidate's page column contents,
        a link to candidate's photo and a link to candidate's program

    """

    response = requests.get(photo_url)
    photo_name = candidate_info_dict['ПІБ'].replace(' ', '_') + '.' + \
        photo_url.split('.')[-1]
    if not os.path.exists(PATH_TO_LOCATION_WITH_PHOTOS):
        os.makedirs(PATH_TO_LOCATION_WITH_PHOTOS)
    with open((PATH_TO_LOCATION_WITH_PHOTOS + photo_name), 'wb') as f:
        f.write(response.content)
    print('Завантажена фотографія {}'.format(candidate_info_dict['ПІБ']))


def download_candidate_program(program_url, candidate_info_dict):
    """Function downloads candidate's program.

    In case there is no catalogue to save program into, creates one. The name
    of program is candidate's name plus file extension.

    Parameters
    ----------
    program_url: string
        a link to candidate's program, received from candidate_info_dict
    candidate_info_dict: OrderedDict
        a dictionary with candidate's page column contents,
        a link to candidate's photo and a link to candidate's program

    """

    response = requests.get(program_url)
    program_name = candidate_info_dict['ПІБ'].replace(' ', '_') + '.' + \
        program_url.split('.')[-1]
    if not os.path.exists(PATH_TO_LOCATION_WITH_PROGRAMS):
        os.makedirs(PATH_TO_LOCATION_WITH_PROGRAMS)
    with open((PATH_TO_LOCATION_WITH_PROGRAMS + program_name), 'wb') as f:
        f.write(response.content)
    print('Завантажена програма {}'.format(candidate_info_dict['ПІБ']))


def get_csv_with_candidates_info(candidates_info_dicts):
    """Function makes an one csv-file with all information and downloads it.

    Parameters
    ----------
    candidates_info_dicts: list
        a list with candidate_info_dict OrderedDicts

    """

    cands_info = pd.concat(candidates_info_dicts, ignore_index=True)
    cands_info.index.name = 'id'
    cands_info.to_csv('output.csv')


def main():
    """The main function, which leads workflow."""

    candidates_info_dicts = []
    districts = get_links_to_districts()
    for i, district in enumerate(districts, start=11):
        candidates_on_districts = \
            get_links_to_candidates_on_districts(i, district)
        for candidate in candidates_on_districts:
            candidate_info_dict = get_candidate_info(candidate)
            download_candidate_photo(candidate_info_dict['Фотографія'],
                                     candidate_info_dict)
            download_candidate_program(
                candidate_info_dict['Передвиборна програма'],
                candidate_info_dict)
            candidates_info_dicts.append(pd.DataFrame(candidate_info_dict,
                                         index=[0]))
    get_csv_with_candidates_info(candidates_info_dicts)


if __name__ == '__main__':
    print('Починаю завантаження')
    try:
        main()
    except:
        print('Помилка')
    else:
        print('Завантаження успішне')
