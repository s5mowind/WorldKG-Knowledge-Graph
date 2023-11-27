import osmium
import pandas as pd
import re
import sys
import urllib
import time
from rdflib import Graph, Namespace, URIRef, Literal
from tqdm import tqdm
from datetime import timedelta
import argparse

class osm2rdf_handler(osmium.SimpleHandler):
    def __init__(self, feature_file:str, key_file:str, min_tags:int):
        osmium.SimpleHandler.__init__(self)
        self.pbar = tqdm(desc='- Processing Nodes')
        self.counts = 0

        # prepare Graph namespace
        self.g = Graph()
        self.graph = self.g
        self.wd = Namespace("http://www.wikidata.org/wiki/")
        self.g.bind("wd", self.wd)
        self.wdt = Namespace("http://www.wikidata.org/prop/direct/")
        self.g.bind("wdt", self.wdt)
        self.wkg = Namespace("http://www.worldkg.org/resource/")
        self.g.bind("wkg", self.wkg)
        self.wkgs = Namespace("http://www.worldkg.org/schema/")
        self.g.bind("wkgs", self.wkgs)
        self.geo = Namespace("http://www.opengis.net/ont/geosparql#")
        self.g.bind("geo", self.geo)
        self.rdfs = Namespace('http://www.w3.org/2000/01/rdf-schema#')
        self.g.bind("rdfs", self.rdfs)
        self.rdf = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
        self.g.bind('rdf', self.rdf)
        self.ogc = Namespace("http://www.opengis.net/rdf#")
        self.g.bind('ogc', self.ogc)
        self.sf = Namespace("http://www.opengis.net/ont/sf#")
        self.g.bind('sf', self.sf)
        self.osmn = Namespace("https://www.openstreetmap.org/node/")
        self.g.bind("osmn", self.osmn)

        self.namespace = {key: uri for key, uri in self.g.namespaces()}

        # load osm features and keys
        self.supersub = pd.read_csv(feature_file, sep='\t', encoding='utf-8')
        self.key_list = pd.read_csv(key_file, sep='\t', encoding='utf-8')
        self.key_list = list(self.key_list['key'])
        self.supersub = self.supersub.drop_duplicates()
        
        self.dict_class = self.supersub.groupby('key')['value'].apply(list).reset_index(name='subclasses').set_index('key').to_dict()['subclasses']

        self.min_tags = min_tags
    
    def to_camel_case_class(self, word):
        word = word.replace(':', '_')
        return ''.join(x.capitalize() or '_' for x in word.split('_'))
    
    def to_camel_case_classAppend(self, key, val):
        return self.supersub.loc[(self.supersub['value'] == val) & (self.supersub['key'] == key)]['appendedClass'].values[0]
    
    def to_camel_case_key(self, input_str):
        input_str = input_str.replace(':', '_')
        words = input_str.split('_')
        return words[0] + "".join(x.title() for x in words[1:])
    
    def printTriple(self, s, p, o):
        if p in self.dict_class: 
            if o in self.dict_class[p]:
                rel = self.namespace['wkg'] + s
                instanceOf = self.namespace['rdf'] + 'type'
                res = self.namespace['wkgs'] + self.to_camel_case_classAppend(p, o)
                self.g.add((rel, instanceOf, res))
            if o == 'Yes':
                rel = self.namespace['wkg'] + s
                instanceOf = self.namespace['rdf'] + 'type'
                res = self.namespace['wkgs'] + self.to_camel_case_class(p)
                self.g.add((rel, instanceOf, res))
        else:
            if p=='Point':
                sub = self.namespace['wkg'] + s
                geoprop = self.namespace['wkgs'] + 'spatialObject'
                geoobj = self.namespace['wkg'] + 'geo' + s
                prop = self.namespace['sf'] + 'Point'
                typ = self.namespace['rdf'] + 'type'
                self.g.add((sub, geoprop, geoobj))
                self.g.add((geoobj, typ, prop))
                self.g.add((geoobj, self.geo["asWKT"], Literal(o, datatype=self.geo.wktLiteral)))
            elif p == 'osmLink':
                sub = self.namespace['wkg'] + s
                prop = self.namespace['wkgs'] + 'osmLink'
                obj = self.namespace['osmn'] + o
                self.g.add((sub, prop, obj))
            elif p == 'name':
                sub = self.namespace['wkg'] + s
                prop = self.namespace['rdfs'] + 'label'
                self.g.add((sub, prop, Literal(o)))
            elif p == 'wikidata':
                sub = self.namespace['wkg'] + s
                prop = self.namespace['wkgs'] + p
                if re.match(r'^Q[0-9]+$', o):
                    obj = self.namespace['wd'] + o
                else:
                    obj = Literal(o)
                self.g.add((sub, prop, obj))
            elif p == 'wikipedia':
                sub = self.namespace['wkg'] + s
                prop = self.namespace['wkgs'] + 'wikipedia'
                try:
                    country = o.split(':')[0]
                    ids = o.split(':')[1]
                except IndexError:
                    country = ''
                    ids = o
                url = country+'.wikipedia.org/wiki/'+country+':'+ids
                url = 'https://'+urllib.parse.quote(url)
                obj = URIRef(url)
                self.g.add((sub, prop, obj))
            else:
                if p in self.key_list:
                    sub = self.namespace['wkg'] + s
                    prop = self.namespace['wkgs'] + self.to_camel_case_key(p)
                    self.g.add((sub, prop, Literal(o)))
        
    def __close__(self):
        print(str(self.counts))

    def node(self, n):
        self.pbar.update(1)
        if len(n.tags) > self.min_tags:
            id = str(n.id)



            point = 'Point('+str(n.location.lon)+' '+str(n.location.lat)+')'

            self.printTriple(id, "Point", point)
            self.printTriple(id, "osmLink", id)

            for k, v in n.tags:

                val = str(v)

                val = val.replace("\\", "\\\\")
                val = val.replace('"', '\\"')
                val = val.replace('\n', " ")

                k = k.replace(" ", "")

                self.printTriple(id, k, val)

start = time.time()

parser = argparse.ArgumentParser()
parser.add_argument('--osm_features', type=str, default='required files/OSM_Ontology_map_features.csv')
parser.add_argument('--key_list', type=str, default='required files/Key_List.csv')
parser.add_argument('--output_file', type=str, default='data/graph.ttl')
parser.add_argument('--input_file', type=str, required=True)
parser.add_argument('--min_tags', type=int, default=2)

args = parser.parse_args()

try:
    with open(args.output_file, 'w') as file:
        pass
except IOError as err:
    sys.exit(f'can not write to {args.output_file}')

print('Compute WorldKG Triples:')
print(f'- reading from {args.input_file}')
print(f'- will write to {args.output_file}')

h = osm2rdf_handler(args.osm_features, args.key_list, args.min_tags)
h.apply_file(args.input_file)
h.pbar.close()
print(f'- writing to {args.output_file}')
h.graph.serialize(args.output_file, format="turtle", encoding="utf-8")

end = time.time()

print(f"- Total runtime: {timedelta(seconds=end - start)}")
