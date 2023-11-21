import os
import argparse
import time
from datetime import timedelta

parser = argparse.ArgumentParser()
parser.add_argument('--input_file', type=str, required=True, help='PBF file containing OSM data to parse to WorldKG')
parser.add_argument('--fasttext_file', required=True, type=str, help='location of fasttext binaries, if none present fasttext will be downloaded to this location')
args = parser.parse_args()

start = time.time()
print('Start generating WorldKG Graph')

if not os.path.exists('data/'):
    print('- creating data directory')
    os.makedirs('data')

os.system(f"python CreateTriples.py --input_file {args.input_file}")
os.system("python \"generate entities.py\"") # ToDo integrate into CreateTriples to reduce graph loading times
os.system(f"python \"generate embeddings.py\" --fasttext_file {args.fasttext_file}")
os.system("python \"match entities.py\"")
os.system("python \"update graph.py\"")

print('Finished generation')
end = time.time()

print(f"- Total runtime: {timedelta(seconds=end - start)}")