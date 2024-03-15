# WorldKG-Knowledge-Graph 

- [www.worldkg.org](http://www.worldkg.org/)

This repository contains code to reproduce the WorldKG knowledge graph. 
The WorldKG knowledge graph is a comprehensive large-scale geospatial knowledge graph based on OpenStreetMap that provides semantic representation of geographic entities from over 188 countries. WorldKG contains higher representation of geographic entities compared to other knowledge graph and can be used as an underlying data source for various applications such as geospatial question answering, geospatial data retrieval, and other cross-domain semantic data-driven applications.

### Prerequisites

- Python >= 3.7


### Setup
Steps to create the WorldKG triples for the particular OSM snapshot:
- Install the Python Requirements: `pip install -r requirements.txt`

#### Create basic triples  
The basic WorldKG triples can still be created by running the create_triples script:  
```
python create_triples.py /path-to-pbf-file /path-to-the-ttl-file-to-save-triples 
```

#### Create connected triples  
The full WorldKG pipeline creates triples and creates connections from relations to static strings by replacing the string with the corresponding entity.
This is achieved with an unsupervised similarity score. To run the full pipeline proceed as follows:  
Either download the OpenStreetMap snapshot from any source of choice or use the download feature from the `worldkg.py` script.  
Either download the FastText Binary from any source of choice or use the download feature from the `worldkg.py` script.  
Run the `worldkg.py` script as:  
*without download:*
```
python worldkg.py --input_file /path-to-pbf-file --fasttext_file /path-to-fasttext-file
```
*with download:*
```
python worldkg.py --download_osm --geofabrik_name continent/country --download_fasttext
```
*Script parameters:*  
- `--input_file`: path to the pbf file containing the osm data 
- `--fasttext_file`: path to the file containing the fasttext binary 
- `--output_file`: path to write the final updated graph to (default: `updated_graph.ttl`) 
- `--download_osm`: toggle direct download of osm files from https://download.geofabrik.de/ 
- `--geofabrik_name`: which osm file to download. Selected according to geofabrik website structure. For a whole continent such as africa use e.g. `africa`. For a country within a continent check the website and specify like `europe/germany` 
- `--download_fasttext`: toggle direct download of fasttext binary from https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.en.300.bin.gz 
- `--cut_off`: minimum similarity to create a link between entities. Select from the range between 1 and 2

#### Useful additional features  
The script `bulk_load.py` allows for processing multiple runs in a row. Either use the predefined lists for small countries in europe and asia, provide files from a directory, or download a list of references from geofabrik.
Files are downloaded first and then processed to prevent having to alter the input list, when errors occurr in linking.  
`replace_prefixes.py` changes the prefixes in a ttl file that has been computed already. To replace a prefix specify the prefix as well as the replacement value as such: `wkg=http://worldkg-dsis.iai.uni-bonn.de:8894/resource/`  
For faster processing triplets can be created individually and joined later. To join ttl files use the `join_ttlfiles.py` script. A change of prefixes can also be specified for the join.

#### Data    
When using the full WorldKG pipeline, intermediate states of the pipeline are written to the data folder. The initial triplets and a csv containing all matched entities and their confidence score can be found here.

### Reference:
If you find our work useful in your research please consider citing our paper.

```
@inproceedings{dsouza2021worldkg,
   title={{WorldKG: A World-Scale Geographic Knowledge Graph}},
   author={Dsouza, Alishiba and Tempelmeier, Nicolas and Yu, Ran and Gottschalk, Simon and Demidova, Elena},
   booktitle={{CIKM} '21: The 30th {ACM} International Conference on Information and Knowledge Management},
   year={2021},
   publisher={{ACM}},
   doi={10.1145/3459637.3482023}
}
```

# License
MIT License

Copyright (c) 2021 Alishiba Dsouza

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
