import pandas as pd
import numpy as np
import pygeohash
import re
import gensim
import requests
import os
import json
import argparse

def wkt_to_geohash(wkt:str, precision:int=6) -> str:
    """
    encode string in wkt format to geohash
    :param wkt: location encoded in wkt format
    :return: geohash encoded location string
    """
    m = re.match(r'Point\((.*) (.*)\)', wkt)
    if m:
        lon = float(m.group(1))
        lat = float(m.group(2))
        return pygeohash.encode(longitude=lon, latitude=lat, precision=precision)
    else:
        return '000000'

def generate_embedding(sentence:str, model:gensim.models.fasttext) -> np.array:
    """
    generate average embedding for list of strings
    :param sentence: string to embed
    :param model: model to generate embeddings from
    :return: average embedding of all words
    """
    embeddings = []
    words = sentence.split(' ')  # splitting to avoid parsing as subwords
    for word in words:
        if word in model.wv:
            embeddings.append(model.wv[word])
        else:
            embeddings.append(np.zeros(300))
    return np.mean(embeddings, axis=0).tolist()

def generate_tail_label_embedding(label:str, name:str, model:gensim.models.fasttext) -> np.array:
    """
    wrapper function to conditionally combine label and name embeddings for tail candidates
    :param label: string containing label
    :param name: string containing name
    :param model: fasttext model used to generate embeddings
    :return: conditionally, average embedding of labels
    """
    embedding = generate_embedding(label, model)
    if name != '<UNK>':
        embedding = np.mean([embedding, generate_embedding(name, model)], axis=0)
    return embedding

parser = argparse.ArgumentParser()
parser.add_argument('--fasttext_file', required=True, type=str, help='location of fasttext binaries, if none present fasttext will be downloaded to this location')
parser.add_argument('--candidate_input', type=str, default='data/candidates.parquet.zip')
parser.add_argument('--subject_input', type=str, default='data/subjects.parquet.zip')
parser.add_argument('--candidate_output', type=str, default='data/candidates_embedding.parquet.zip')
parser.add_argument('--subject_output', type=str, default='data/subjects_embedding.parquet.zip')
parser.add_argument('--predicate_map', type=str, default='data/predicate_map.json')
parser.add_argument('--literal_map', type=str, default='data/literal_map.json')
parser.add_argument('--type_map', type=str, default='data/type_map.json')
args = parser.parse_args()

print('Embedding generation starting:')


# download fasttext binaries if unavailable
if not os.path.exists(args.fasttext_file):
    print(f'- no fasttext binary in {os.path.dirname(args.fasttext_file)}')
    print('- downloading fasttext binaries')
    r = requests.get('https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.en.300.bin.gz')
    with open(args.fasttext_file, 'wb') as f:
        f.write(r.content)
    print('- finished download')
print('- loading fasttext model, this may take a while')
ft_model = gensim.models.fasttext.load_facebook_model(args.fasttext_file)

print('- generating candidate embeddings')
candidates = pd.read_parquet(args.candidate_input)

col_names = list(candidates.columns) + ['geohash'] + [f'label_emb{i}' for i in range(300)]
# generate geohash encoding for location
geoh = candidates.apply(lambda row: wkt_to_geohash(row['location']), axis=1)
# generate label embeddings per entry
label_emb = candidates.apply(lambda row: generate_tail_label_embedding(row['label'], row['label_en'], ft_model), axis=1, result_type='expand')
candidates = pd.concat([candidates, geoh, label_emb], axis=1)
candidates.columns = col_names
# generate embeddings for unique types to reduce compuatation cost
type_map = {obj_type: np.zeros(300).tolist() if obj_type == '<UNK>' else generate_embedding(obj_type, ft_model) for obj_type in candidates['type'].unique()}

candidates.to_parquet(args.candidate_output, compression='gzip', engine='pyarrow')

with open(args.type_map, 'w') as f:
    json.dump(type_map, f)

print('- generating subject embeddings')
subjects = pd.read_parquet(args.subject_input)

# generate geohash encoding for location
subjects['geohash'] = subjects.apply(lambda row: wkt_to_geohash(row['location']), axis=1)
# generate embeddings for unique predicates to reduce compuatation cost
predicate_map = {pred: generate_embedding(pred.split(':')[-1], ft_model) for pred in subjects['predicate'].unique()}
# generate embeddings for unique literals to reduce compuatation cost
literal_map = {literal: generate_embedding(literal, ft_model) for literal in subjects['literal'].unique()}

subjects.to_parquet(args.subject_output, compression='gzip', engine='pyarrow')

with open(args.predicate_map, 'w') as f:
    json.dump(predicate_map, f)

with open(args.literal_map, 'w') as f:
    json.dump(literal_map, f)
