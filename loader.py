# -*- coding: utf-8 -*-
import csv
import datetime
import json
import os,ssl
import urllib.request


import elasticsearch.helpers
from elasticsearch import Elasticsearch, RequestsHttpConnection, serializer, compat, exceptions

if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context


TYPE = 'record'


# see https://github.com/elastic/elasticsearch-py/issues/374
class JSONSerializerPython2(serializer.JSONSerializer):
    """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
    See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
    A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
    as ascii can be found here: https://docs.python.org/2/library/json.html#basic-usage
    """

    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)


class ESLoader(object):
    def __init__(self, file_location, index_name, drop_existing=False, alias=None, host='localhost:9200'):
        """
        :param file_location 
        :param index_name: the es index to upload to
        :param drop_existing:
        :param alias: the es alias to associate the index with
        """
        self.file_location =  file_location
        self.index_name = index_name
        self.drop_existing = drop_existing
        self.alias = alias
        self.es = Elasticsearch([host], serializer=JSONSerializerPython2())

    def load(self):
        if not self.es.indices.exists(self.index_name):
            print ('creating index ' + self.index_name)
            self.__create_index()
        elif self.drop_existing:
            print('deleting index ' + self.index_name)
            self.es.indices.delete(index=self.index_name)
            print ('creating index ' + self.index_name)
            self.__create_index()
        
        print('indexing ' + self.file_location)
        
        try:
            self.__load_file(self.file_location)
        except RuntimeError as e:
            print(e)
            print("Failed to load file {}".format(file))

        print("Indexed " + self.file_location)

    def __load_file(self, file):
        doc_count = 0
        data = []

        with open(file) as f:
            print("Starting indexing on " + f.name)
            reader = csv.DictReader(f)

            for row in reader:
                # gracefully handle empty locations
                if (row['decimalLatitude'] == '' or row['decimalLongitude'] == ''): 
                    row['location'] = ''
                else:
                    row['location'] = row['decimalLatitude'] + "," + row['decimalLongitude'] 

                # pipeline code identifies null yearCollected values as 'unknown'. es_loader should be empty string
                if (row['yearCollected'] == 'unknown'): 
                    row['yearCollected'] = ''
                if (row['yearCollected'] == 'Unknown'): 
                    row['yearCollected'] = ''

                data.append({k: v for k, v in row.items() if v})  # remove any empty values

            elasticsearch.helpers.bulk(client=self.es, index=self.index_name, actions=data, 
                                       raise_on_error=True, chunk_size=10000, request_timeout=60)
            doc_count += len(data)
            print("Indexed {} documents in {}".format(doc_count, f.name))

        return doc_count

    def __create_index(self):
        request_body = {
            "mappings": {
                    "properties": {
                        "materialSampleID": {"type": "keyword"},
                        "principalInvestigator": {"type": "keyword"},
                        "diseaseTested": {"type": "keyword"},
                        "diseaseDetected": {"type": "keyword"},
                        "country": {"type": "keyword"},
                        "basisOfRecord": {"type": "keyword"},
                        "order": {"type": "keyword"},
                        "family": {"type": "keyword"},
                        "genus": {"type": "keyword"},
                        "fatal": {"type": "keyword"},
                        "habitat": {"type": "keyword"},
                        "sampleType": {"type": "keyword"},
                        "institutionCode": {"type": "keyword"},
                        "collectionCode": {"type": "keyword"},
                        "decimalLatitude": { "type": "float" },
                        "decimalLongitude": { "type": "float" },
                        "location": { "type": "geo_point" },                        
                        "yearCollected": {"type": "integer"},
                        "monthCollected": {"type": "integer"},
                        "specificEpithet": {"type": "text"},
                        "catalogNumber": {"type": "text"},
                        "collectorList": {"type": "text"},
                        "Sample_bcid": {"type": "text"}
                    }
            }
        }
        self.es.indices.create(index=self.index_name, body=request_body)
        
def get_files(dir, ext='csv'):
    for root, dirs, files in os.walk(dir):

        if len(files) == 0:
            print("no files found in {}".format(dir))

        for file in files:
            if file.endswith(ext):
                yield os.path.join(root, file)    

index = 'amphibiandisease'
drop_existing = True
alias = 'amphibiandisease'
host =  'tarly.cyverse.org:80'
#file_location = 'test.csv'
file_location = 'data/amphibian_disease_data_processed.csv'

loader = ESLoader(file_location, index, drop_existing, alias, host)
loader.load()


