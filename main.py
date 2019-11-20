"""CEC scrape module.

It's a try to make a module for scraping information
from CEC website on any elections you want

"""

import argparse
import json

parser = argparse.ArgumentParser(
    prog='Scrape CEC',
    description="It's a try to make a utility for scraping information \
        from CEC website on any elections you want",
    usage='%(prog)s [options]')
parser.add_argument(
    '-y', '--year',
    type=str,
    nargs='+',
    choices=['2004', '2006', '2007', '2010', '2012', '2014', '2015', '2019'],
    required=True,
    help='an election year you want to scrape')
parser.add_argument(
    '-e', '--elections',
    type=str,
    nargs='+',
    choices=['presedential', 'parliamentary', 'local'],
    required=True,
    help='select the election type you want to scrape')
args = parser.parse_args()

year = (year for year in args.year)
elections = (elections for elections in args.elections)
with open('settings.json', 'r') as settings_file:
    settings = json.load(settings_file)
