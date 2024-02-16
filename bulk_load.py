import os
import argparse
import requests
import re

import time
from datetime import timedelta

parser = argparse.ArgumentParser()
group_fasttext = parser.add_mutually_exclusive_group(required=True)
group_fasttext.add_argument('--download_fasttext', action='store_true', default=False, help='toggle direct download of fasttext from fbai')
group_fasttext.add_argument('--fasttext_file', type=str, help='location of fasttext binaries')

parser.add_argument('--from_directory', type=str, default='', help='run on directory of pbf files instead of downloading from geofabrik')

continent_arg = parser.add_argument('--geofabrik_continent', type=str, help='geofabrik continent prefix to use')
group_countries = parser.add_mutually_exclusive_group(required=False)
group_countries.add_argument('--custom_countries', nargs='+', type=str, help='list of countries to run in bulk')
group_countries.add_argument('--use_predefined', action='store_true', default=False, help='use countries that are preselected in this script')

parser.add_argument('--output_directory', type=str, required=True, help='Target directory for storing resulting ttl files.')
parser.add_argument('--cut_off', default=1.5, type=float, help='minimum score to achieve for predicted links to be considered')
args = parser.parse_args()


if args.from_directory:
    countries = []
    for s in os.listdir(args.from_directory):
        if re.match(r'-latest.osm.pbf$', s):
            countries.append(s.replace(r'-latest.osm.pbf$', ''))
    print('found the following pbf files:', countries)
else:
    if args.use_predefined:
        match args.geofabrik_continent:
            case 'asia':
                countries = ['afghanistan', 'armenia', 'azerbaijan', 'bangladesh', 'bhutan', 'cambodia', 'gcc-states',
                             'iran', 'iraq', 'israel-and-palestine', 'jordan', 'kazakhstan', 'kyrgyzstan', 'laos',
                             'lebanon', 'malaysia-singapore-brunei', 'maldives', 'mongolia', 'myanmar', 'nepal',
                             'north-korea', 'pakistan', 'philippines', 'south-korea', 'sri-lanka', 'syria', 'taiwan',
                             'tajikistan', 'thailand', 'turkmenistan', 'uzbekistan', 'vietnam', 'yemen']
            case 'europe':
                countries = ['albania', 'andorra', 'azores', 'belarus', 'bosnia-herzegovina', 'bulgaria', 'croatia',
                             'cyprus', 'estonia', 'faroe-islands', 'finland', 'georgia', 'greece', 'guernsey-jersey',
                             'hungary', 'iceland', 'ireland-and-northern-ireland', 'isle-of-man', 'kosovo', 'latvia',
                             'liechtenstein', 'lithuania', 'luxembourg', 'macedonia', 'malta', 'moldova', 'monaco',
                             'montenegro', 'portugal', 'romania', 'serbia', 'slovakia', 'slovenia', 'sweden',
                             'switzerland', 'turkey', 'ukraine']
            case _:
                raise argparse.ArgumentError(continent_arg, 'no predefined countries present')
    else:
        countries = args.custom_countries

    # Dowload Files first to make sure no geofabrik references are wrong
    print('Bulk Download Started:')
    for country in countries:
        print(f'Downloading OSM File: {country}')
        r = requests.get(f'https://download.geofabrik.de/{args.geofabrik_continent}/{country}-latest.osm.pbf')
        with open(f'{os.path.basename(country)}-latest.osm.pbf', 'wb') as f:
            f.write(r.content)
        print(f'- finished download {country}')

# download fasttext binaries
if args.download_fasttext:
    print('- downloading fasttext binaries')
    r = requests.get('https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.en.300.bin.gz')
    with open('cc.en.300.bin.gz', 'wb') as f:
        f.write(r.content)
    print('- finished download')
    ft_file = 'cc.en.300.bin.gz'
else:
    ft_file = args.fasttext_file


start = time.time()

if not os.path.exists('data/'):
    print('- creating data directory')
    os.makedirs('data')

for country in countries[:2]:
    print(f'bulk running: {country}')
    os.system(f"python worldkg.py --fasttext_file {ft_file} --input_file {os.path.join(args.from_directory, f'{country}-latest.osm.pbf')} --output_file {os.path.join(args.output_directory, f'{country}.ttl')}")

end = time.time()
print(f"- Total bulkload runtime: {timedelta(seconds=end - start)}")