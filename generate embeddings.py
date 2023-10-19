import pandas as pd
import numpy as np
import pygeohash
import re
from haversine import haversine
import gensim
import requests
import os
import json

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

print('Embedding generation starting:')

location = 'E:\Datasets/Embeddings'
fasttext_file = 'cc.en.300.bin.gz'

# download fasttext binaries if unavailable
if not os.path.exists(os.path.join(location, fasttext_file)):
    print(f'- no fasttext binary in {location}')
    print('- starting download')
    r = requests.get('https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.en.300.bin.gz')
    with open(os.path.join(location, fasttext_file), 'wb') as f:
        f.write(r.content)
    print('- finished download')
print('- loading fasttext model, this may take a while')
ft_model = gensim.models.fasttext.load_facebook_model(os.path.join(location, fasttext_file))

print('- generating candidate embeddings')
candidates = pd.read_parquet('candidates.parquet.gzip')

col_names = list(candidates.columns) + ['geohash'] + [f'label_emb{i}' for i in range(300)]
# generate geohash encoding for location
geoh = candidates.apply(lambda row: wkt_to_geohash(row['location']), axis=1)
# generate label embeddings per entry
label_emb = candidates.apply(lambda row: generate_tail_label_embedding(row['label'], row['label_en'], ft_model), axis=1, result_type='expand')
candidates = pd.concat([candidates, geoh, label_emb], axis=1)
candidates.columns = col_names
# generate embeddings for unique types to reduce compuatation cost
type_map = {obj_type: np.zeros(300).tolist() if obj_type == '<UNK>' else generate_embedding(obj_type, ft_model) for obj_type in candidates['type'].unique()}

candidates.to_parquet('candidates_embedding.parquet.gzip', compression='gzip', engine='pyarrow')

with open('type_map.json', 'w') as f:
    json.dump(type_map, f)

print('- generating subject embeddings')
subjects = pd.read_parquet('subjects.parquet.gzip')

# generate geohash encoding for location
subjects['geohash'] = subjects.apply(lambda row: wkt_to_geohash(row['location']), axis=1)
# generate embeddings for unique predicates to reduce compuatation cost
predicate_map = {pred: generate_embedding(pred.split(':')[-1], ft_model) for pred in subjects['predicate'].unique()}
# generate embeddings for unique literals to reduce compuatation cost
literal_map = {literal: generate_embedding(literal, ft_model) for literal in subjects['literal'].unique()}

subjects.to_parquet('subjects_embedding.parquet.gzip', compression='gzip', engine='pyarrow')

with open('predicate_map.json', 'w') as f:
    json.dump(predicate_map, f)

with open('literal_map.json', 'w') as f:
    json.dump(literal_map, f)
