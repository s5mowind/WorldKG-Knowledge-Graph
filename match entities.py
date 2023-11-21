import json
import pandas as pd
from haversine import haversine
from pygeohash import decode_exactly
from tqdm import tqdm
import csv
from sklearn.metrics.pairwise import cosine_similarity
import argparse

def haversine_from_geohash(hash1:str, hash2:str) -> float:
    """
    function to estimate haversine distance from geohash strings
    :param hash1: first loaction encoded in geohash
    :param hash2: second location encoded in geohash
    :return: estimated distance between locations in km
    """
    # only take first two parts of tuples, rest are error estimations
    hd = haversine(decode_exactly(hash1)[:2], decode_exactly(hash2)[:2])
    return hd


parser = argparse.ArgumentParser()
parser.add_argument('--candidate_file', type=str, default='data/candidates_embedding.parquet.zip')
parser.add_argument('--subject_file', type=str, default='data/subjects_embedding.parquet.zip')
parser.add_argument('--output_file', type=str, default='data/uslp-triplets.csv')
parser.add_argument('--geohash_precision', type=str, default='required files/geohash_precision.json')
parser.add_argument('--predicate_map', type=str, default='data/predicate_map.json')
parser.add_argument('--literal_map', type=str, default='data/literal_map.json')
parser.add_argument('--type_map', type=str, default='data/type_map.json')

args = parser.parse_args()

print('Matching entities:')

candidates = pd.read_parquet(args.candidate_file)
subjects = pd.read_parquet(args.subject_file)

# precisions to use when comparing distances
with open(args.geohash_precision, 'r') as f:
    geohash_precision_map = json.load(f)

with open(args.predicate_map, 'r') as f:
    predicate_map = json.load(f)
with open(args.literal_map, 'r') as f:
    literal_map = json.load(f)
with open(args.type_map, 'r') as f:
    type_map = json.load(f)

predicates = pd.DataFrame.from_dict(predicate_map, orient='index')
literals = pd.DataFrame.from_dict(literal_map, orient='index')
types = pd.DataFrame.from_dict(type_map, orient='index')

print('- computing similarity scores')
# cosine similarity matrix for similarity between literals and names
cos_sim_literal = cosine_similarity(candidates.loc[:, [f'label_emb{i}' for i in range(300)]], literals.values)
literal_cossim_df = pd.DataFrame(cos_sim_literal, index=candidates.index.to_list(), columns=literals.index)

# cosine similarity matrix for similarity between predicates and types
cos_sim_predicate = cosine_similarity(types.values, predicates.values)
predicate_cossim_df = pd.DataFrame(cos_sim_predicate, index=types.index, columns=predicates.index)

# precompute containment of candidate types in predicates
contains_map = {}
for predicate in tqdm(predicate_map.keys(), desc='- Computing type containment'):
    similarities = {}
    for t in type_map.keys():
        similarity = 0
        if t != '<UNK>':
            if str(t) in predicate:
                similarity = 0.5
        similarities.update({t: similarity})
    contains_map.update({predicate: similarities})
type_contained = pd.DataFrame.from_dict(contains_map, orient='index')


print('- the following distance matrices will be used')
for precision in set(geohash_precision_map.values()):
    print(f"- distance matrix prec = {precision}: ({len(candidates.apply(lambda row: row['geohash'][:precision], axis=1).unique())}, {len(subjects.apply(lambda row: row['geohash'][:precision], axis=1).unique())})")

# precompute distances for faster access
distance_matrices = {}
for p in tqdm(set(geohash_precision_map.values()), desc='- Computing distance matrices'):
    # compute distances between unique hash value combinations
    dist_mat = {hash1: {hash2: haversine_from_geohash(hash1, hash2) for hash2 in candidates.apply(lambda row: row['geohash'][:p], axis=1).unique()} for hash1 in subjects.apply(lambda row: row['geohash'][:p], axis=1).unique()}
    # pass into dataframe and normalize
    distance_frame = pd.DataFrame.from_dict(dist_mat, orient='index')
    max_val = distance_frame.values.max()
    if max_val > 0:
        distance_frame = pd.DataFrame(1 - (distance_frame.values / max_val), columns=distance_frame.columns, index=distance_frame.index)
    distance_matrices.update({p: distance_frame})

matched = {}
pairs = []
for index, row in tqdm(subjects.iterrows(), total=len(subjects), desc='- Finding matches'):
    # save core properties for repetitive access
    precision = geohash_precision_map[row['predicate']]
    gh = row['geohash'][:precision]
    pred = row['predicate']
    lit = row['literal']
    if (gh, pred, lit) in matched:  # only consider new constellations for subjects
        # if these three values are the same, the uslp-score will also be the same
        pairs.append((row['uri'], pred, lit) + matched[(gh, pred, lit)])
    else:
        # prepare similarity and distance matrices for repetitive access
        dm_precision = distance_matrices[precision].loc[gh, :]
        cossim_matching_predicate = predicate_cossim_df.loc[:, pred]
        cossim_matching_literal = literal_cossim_df.loc[:, lit]
        contains_matching_predicate = type_contained.loc[pred, :]

        # iterate through candidates and compute uslp-score
        candidate_eval = candidates.apply(
            lambda cand: dm_precision.loc[cand['geohash'][:precision]]
                         + cossim_matching_predicate.loc[cand['type']]
                         + cossim_matching_literal.loc[cand.name]
                         + contains_matching_predicate.loc[cand['type']],
            axis=1
        )

        # find best candidate
        best_candidate = candidate_eval.idxmax()
        best_candidate_uri = candidates.loc[best_candidate, 'uri']
        best_candidate_score = candidate_eval[best_candidate]

        # store matches and save constellation
        pairs.append((row['uri'], pred, lit, best_candidate_uri, candidate_eval[best_candidate]))
        matched.update({(gh, pred, lit): (best_candidate_uri, best_candidate_score)})

print(f'- {(len(subjects) - len(matched))/len(subjects)*100:.2f}% of computations performed with dictionary')

print(f'- saving results to {args.output_file}')
predictions = pd.DataFrame(pairs, columns=['s', 'p', 'literal', 'o', 'score'])
predictions.to_csv(args.output_file, index=False)
