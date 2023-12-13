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
parser.add_argument('--geofabrik_name', type=str, help='name of pbf file to download, such as europe/liechtenstein or australia-oceania')
parser.add_argument('--output_file', type=str, default='updated_graph.ttl', help='name of file containing connected WorldKG triples')
parser.add_argument('--cut_off', default=1.5, type=float, help='minimum score to achieve for predicted links to be considered')
args = parser.parse_args()


if args.download_osm:
    if args.geofabrik_name:
        print(f'Downloading OSM File: {args.geofabrik_name}')
        r = requests.get(f'https://download.geofabrik.de/{args.geofabrik_name}-latest.osm.pbf')
        with open(f'{os.path.basename(args.geofabrik_name)}-latest.osm.pbf', 'wb') as f:
            f.write(r.content)
        print('- finished download')
        pbf_file = f'{os.path.basename(args.geofabrik_name)}-latest.osm.pbf'
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

os.system(f"python create_triples.py --input_file {pbf_file}")
os.system("python generate_entities.py")
os.system(f"python generate_embeddings.py --fasttext_file {ft_file}")
os.system("python match_entities.py")
os.system(f"python update_graph.py --output_file {args.output_file} --cut_off {args.cut_off}")

print('Finished generation')
end = time.time()

print(f"- Total runtime: {timedelta(seconds=end - start)}")