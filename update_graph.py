from rdflib import Graph, URIRef, Literal
from tqdm import tqdm
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--graph_file', default='data/graph.ttl', type=str, help='ttl file containing the graph to update')
parser.add_argument('--prediction_file', default='data/uslp-triplets.csv', type=str, help='csv file containing uslp predictions')
parser.add_argument('--output_file', default='updated_graph.ttl', type=str, help='file location to write updated graph to')
parser.add_argument('--cut_off', default=1.5, type=float, help='minimum score to achieve for updates to occur')
args = parser.parse_args(args=[])

# import unlinked knowledge graph
print('- loading Graph Data')
g = Graph()
g.parse(args.graph_file)

# import uslp predictions
predictions = pd.read_csv(args.prediction_file)
predictions = predictions[['s', 'p', 'o', 'literal', 'score']]

# extract namespace for uri reconstruction
namespace = {key: uri for key, uri in g.namespaces()}

def cast_uri(term:str, namespace:dict) -> URIRef:
    """
    reconstruct term in prefix:fragment form into full uri and create URIRef Object
    :param term: term in prefix:fragment form
    :param namespace: dictionary containing URIRef objects ordered by their associated short forms
    :return: URIRef Object containing the reconstructed full URI
    """
    prefix, fragment = term.split(':')
    return namespace[prefix] + fragment # select URIRef object and append fragment

# replace old triplets with new predictions
for idx, row in tqdm(predictions.iterrows(), total=len(predictions), desc='- removing old and inserting new triplets'):
    if row['score'] > args.cut_off:
        subject = cast_uri(row['s'], namespace)
        predicate = cast_uri(row['p'], namespace)
        obj = cast_uri(row['o'], namespace)
        literal = Literal(row['literal'])
        g.remove((subject, predicate, literal))
        g.add((subject, predicate, obj))

print(f'- store changed Graph to {args.output_file}')
g.serialize(args.output_file, format="turtle", encoding="utf-8")
