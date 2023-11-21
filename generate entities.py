from rdflib import Graph
from tqdm import tqdm
import pandas as pd
import argparse

print('Retrieving Spatial Entities:')

parser = argparse.ArgumentParser()
parser.add_argument('--candidate_file', type=str, default='candidates.parquet.zip')
parser.add_argument('--subject_file', type=str, default='subjects.parquet.zip')
parser.add_argument('--graph_file', default='slovenia.ttl', type=str, help='ttl file containing the graph to read')
parser.add_argument('--relation_file', default='relations.csv', type=str, help='file containing spatial predicates to predict matches for')

args = parser.parse_args()

print('- loading Graph Data')
g = Graph()
g.parse(args.graph_file)

spatial_object_query = """
PREFIX wkg: <http://www.worldkg.org/resource/>
PREFIX wkgs: <http://www.worldkg.org/schema/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?item ?name ?pos ?type ?nameEn
WHERE {
?item wkgs:spatialObject ?obj.
?item rdfs:label ?name.
?obj geo:asWKT ?pos.
OPTIONAL { ?item rdf:type ?type .}
OPTIONAL { ?item wkgs:nameEn ?nameEn}
}
"""

print('- retrieving candidates')
spatial_objects = []
for r in g.query(spatial_object_query):
    row = {'uri': f"wkg:{r['item'].split('/')[-1]}",
           'label': r['name'],
           'location': r['pos'],
           'type': r['type'].split('/')[-1] if r['type'] else '<UNK>',
           'label_en': r['nameEn'] if r['nameEn'] else '<UNK>'}
    spatial_objects.append(row)
spat_obj_df = pd.DataFrame(spatial_objects)  # these spatial objects will be candidates

spat_obj_df.to_parquet(args.candidate_file, compression='gzip', engine='pyarrow')

# predefined spatial relations to scan for
wkg_relations = pd.read_csv(args.relations_file)

# get all entities and targets with relations
query_relation = """
PREFIX wkg: <http://www.worldkg.org/resource/>
PREFIX wkgs: <http://www.worldkg.org/schema/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?item ?o ?pos ?type
WHERE {
?item wkgs:%s ?o.
?item wkgs:spatialObject ?geoObj.
?geoObj geo:asWKT ?pos .
}
"""

print('- retrieving subjects')
relation_list = []
pbar = tqdm(wkg_relations['relations'])
for relation in pbar:
    pbar.set_postfix_str(relation.split(':')[-1])
    for r in g.query(query_relation % relation.split(':')[-1]):
        row = {'uri': f"wkg:{r['item'].split('/')[-1]}",
               'predicate': relation,
               'literal': r['o'],
               'location': r['pos']}
        relation_list.append(row)
relation_df = pd.DataFrame(relation_list)  # subjects to look for partner for (heads)

relation_df.to_parquet(args.relation_file, compression='gzip', engine='pyarrow')
