import os
import argparse
import requests
import sys

import time
from datetime import timedelta

parser = argparse.ArgumentParser()
group_input = parser.add_mutually_exclusive_group(required=True)
group_input.add_argument('--input_file', type=str, help='PBF file containing OSM data to parse to WorldKG')
group_input.add_argument('--download_osm', action='store_true', default=False, help='toggle direct download of osm file from geofabrik')
group_fasttext = parser.add_mutually_exclusive_group(required=True)
group_fasttext.add_argument('--download_fasttext', action='store_true', default=False, help='toggle direct download of fasttext from fbai')
group_fasttext.add_argument('--fasttext_file', type=str, help='location of fasttext binaries')
parser.add_argument('--geofabrik_name', type=str, help='name of pbf file to download, such as liechtenstein or australia-oceania')
args = parser.parse_args()


if args.download_osm:
    if args.geofabrik_name:
        print(f'Downloading OSM File: {args.geofabrik_name}')
        r = requests.get(f'https://download.geofabrik.de/europe/{args.geofabrik_name}-latest.osm.pbf')
        with open(f'{args.geofabrik_name}-latest.osm.pbf', 'wb') as f:
            f.write(r.content)
        print('- finished download')
        pbf_file = f'{args.geofabrik_name}-latest.osm.pbf'
    else:
        raise ValueError('no region name for geofabrik stated')
else:
    pbf_file = args.input_file

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
print('Start generating WorldKG Graph')

if not os.path.exists('data/'):
    print('- creating data directory')
    os.makedirs('data')

os.system(f"python CreateTriples.py --input_file {pbf_file}")
os.system("python \"generate entities.py\"")
os.system(f"python \"generate embeddings.py\" --fasttext_file {ft_file}")
os.system("python \"match entities.py\"")
os.system("python \"update graph.py\"")

print('Finished generation')
end = time.time()

print(f"- Total runtime: {timedelta(seconds=end - start)}")