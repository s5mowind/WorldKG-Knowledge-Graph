from rdflib import Graph
from tqdm import tqdm
import pandas as pd
import argparse

print('Retrieving Spatial Entities:')

parser = argparse.ArgumentParser()
parser.add_argument('--candidate_file', type=str, default='data/candidates.parquet.zip')
parser.add_argument('--subject_file', type=str, default='data/subjects.parquet.zip')
parser.add_argument('--graph_file', default='data/graph.ttl', type=str, help='ttl file containing the graph to read')
parser.add_argument('--relation_file', default='required files/relations.csv', type=str, help='file containing spatial predicates to predict matches for')
parser.add_argument('--class_file', default='required files/relevant_classes.csv', type=str, help='file containing all types relevant for candidates')

args = parser.parse_args()

print('- loading Graph Data')
# we can integrate generate entities into CreateTriples to remove this loadtime for Graph.parse
# we decided against it to keep the different steps distinct and easy to read
g = Graph()
g.parse(args.graph_file)

# all spatial objects with a position as WKT and a label
# these are the candidates that literals can be mapped to
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
?item rdf:type wkgs:%s.
?obj geo:asWKT ?pos.
OPTIONAL { ?item wkgs:nameEn ?nameEn}
}
"""

obj_type = pd.read_csv(args.class_file, header=None, names=['type'])

print('- retrieving candidates')
spatial_objects = []

for class_type in tqdm([s.split('/')[-1] for s in obj_type['type']]):
    for r in g.query(spatial_object_query % class_type):
        row = {'uri': f"wkg:{r['item'].split('/')[-1]}",
               'label': r['name'],
               'location': r['pos'],
               'type': class_type,
               'label_en': r['nameEn'] if r['nameEn'] else '<UNK>'}
        spatial_objects.append(row)
spat_obj_df = pd.DataFrame(spatial_objects)  # these spatial objects will be candidates (tails)

print(f'- number of candidates: {len(spat_obj_df)}')

spat_obj_df.to_parquet(args.candidate_file, compression='gzip', engine='pyarrow')

# predefined spatial relations to scan for
wkg_relations = pd.read_csv(args.relation_file)

# get all entities and targets with predicates regarding the defined spatial relations
# these will be the heads to update from s, p, literal to s, p, o for a more connected knowledge graph
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

print(f'- number of relations: {len(relation_df)}')

relation_df.to_parquet(args.subject_file, compression='gzip', engine='pyarrow')
