from rdflib import Graph
from tqdm import tqdm
import pandas as pd

print('Retrieving Spatial Entities:')

print('- accessing Triplet storage')
# probably easier to insert into virtuoso temporarily
# ToDo: use virtuoso as triplet storage and access
g = Graph()
g.parse('slovenia.ttl')

spatial_object_query = """
PREFIX wkg: <http://www.worldkg.org/resource/>
PREFIX wkgs: <http://www.worldkg.org/schema/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT ?item ?name ?pos
WHERE {
?item wkgs:spatialObject ?obj.
?item rdfs:label ?name.
?obj geo:asWKT ?pos
}
"""

print('- retrieving candidates')
spatial_objects = []
for r in g.query(spatial_object_query):
    row = {'predicate': f"wkg:{r['item'].split('/')[-1]}", 'label': r['name'], 'location': r['pos']}
    spatial_objects.append(row)
spat_obj_df = pd.DataFrame(spatial_objects)  # these spatial objects will be candidates
# prefiltering would be possible

spat_obj_df.to_parquet('candidates.parquet.gzip', compression='gzip', engine='pyarrow')

# predefined spatial relations to scan for
wkg_relations = pd.read_csv('relations.csv')

# get all entities and targets with relations
query_relation = """
PREFIX wkg: <http://www.worldkg.org/resource/>
PREFIX wkgs: <http://www.worldkg.org/schema/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT ?item ?o ?pos
WHERE {
?item wkgs:%s ?o.
?item wkgs:spatialObject ?geoObj.
?geoObj geo:asWKT ?pos 
}
"""

print('- retrieving subjects')
relation_list = []
pbar = tqdm(wkg_relations['relations'])
for relation in pbar:
    pbar.set_postfix_str(relation.split(':')[-1])
    for r in g.query(query_relation % relation.split(':')[-1]):
        row = {'subject': f"wkg:{r['item'].split('/')[-1]}", 'predicate': relation, 'literal': r['o'], 'location': r['pos']}
        relation_list.append(row)
relation_df = pd.DataFrame(relation_list)  # subjects to look for partner for (heads)

relation_df.to_parquet('subjects.parquet.gzip', compression='gzip', engine='pyarrow')
