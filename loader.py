# -*- coding: utf-8 -*-
import csv
import datetime
import json
import os,ssl
import urllib.request


import elasticsearch.helpers
from elasticsearch import Elasticsearch, RequestsHttpConnection, serializer, compat, exceptions, helpers

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
        chunk_size = 100  # Set chunk size to 100 records

        with open(file) as f:
            print("Starting indexing on " + f.name)
            reader = csv.DictReader(f)

            for row in reader:
                # gracefully handle empty locations
                if row['decimalLatitude'] == '' or row['decimalLongitude'] == '':
                    row['location'] = ''
                else:
                    row['location'] = row['decimalLatitude'] + "," + row['decimalLongitude']

                # handle 'unknown' values for yearCollected
                if row['yearCollected'].lower() == 'unknown':
                    row['yearCollected'] = ''

                data.append({k: v for k, v in row.items() if v})  # remove empty values

                # When chunk_size is reached, send bulk data to Elasticsearch
                if len(data) == chunk_size:
                    helpers.bulk( client=self.es, index=self.index_name, actions=data, raise_on_error=True, request_timeout=60)
                    doc_count += len(data)
                    print(f"Indexed {len(data)} documents. Total indexed: {doc_count}")
                    data = []  # Clear the data list for the next chunk

            # Index remaining data if it’s less than chunk_size
            if data:
                helpers.bulk( client=self.es, index=self.index_name, actions=data, raise_on_error=True, request_timeout=60)
                doc_count += len(data)
                print(f"Indexed {len(data)} remaining documents. Total indexed: {doc_count}")

        print("Finished indexing in", f.name)
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
                        "specificEpithet": {"type": "text"},
                        "scientificName": {"type": "text"},
                        "verbatimScientificName": {"type": "text"},
                        "fatal": {"type": "keyword"},
                        "habitat": {"type": "keyword"},
                        "sampleType": {"type": "keyword"},
                        "institutionCode": {"type": "keyword"},
                        "collectionCode": {"type": "keyword"},
                        "catalogNumber": {"type": "text"},
                        "decimalLatitude": { "type": "float" },
                        "decimalLongitude": { "type": "float" },
"coordinateUncertaintyInMeters": {"type":"text"},
"horizontalDatum": {"type":"text"},
"continentOcean": {"type":"text"},
"stateProvince": {"type":"text"},
"municipality": {"type":"text"},
"county": {"type":"text"},
"locationRemarks": {"type":"text"},
"habitat": {"type":"text"},
"eventRemarks": {"type":"text"},
"georeferenceProtocol": {"type":"text"},
"minimumElevationInMeters": {"type":"text"},
"maximumElevationInMeters": {"type":"text"},
"minimumDepthInMeters": {"type":"text"},
"maximumDepthInMeters": {"type":"text"},
"locationID": {"type":"text"},
                        "locality": { "type": "keyword" },                        
                        "location": { "type": "geo_point" },                        
                        "yearCollected": {"type": "integer"},
                        "monthCollected": {"type": "integer"},
"dayCollected": {"type":"text"},
"verbatimEventDate": {"type":"text"},
                        "collectorList": {"type": "text"},
"occurrenceID": {"type":"text"},
"otherCatalogNumbers": {"type":"text"},
"fieldNumber": {"type":"text"},
"associatedReferences": {"type":"text"},
"occurrenceRemarks": {"type":"text"},
"infraspecificEpithet": {"type":"text"},
"taxonRemarks": {"type":"text"},
"lifeStage": {"type":"text"},
"establishmentMeans": {"type":"text"},
"sex": {"type":"text"},
"individualCount": {"type":"text"},
"weightUnits": {"type":"text"},
"weight": {"type":"text"},
"lengthUnits": {"type":"text"},
"length": {"type":"text"},
"diseaseLineage": {"type":"text"},
"genotypeMethod": {"type":"text"},
"testMethod": {"type":"text"},
"diseaseTestedPositiveCount": {"type":"text"},
"specimenDisposition": {"type":"text"},
"quantityDetected": {"type":"text"},
"dilutionFactor": {"type":"text"},
"cycleTimeFirstDetection": {"type":"text"},
"zeScore": {"type":"text"},
"diagnosticLab": {"type":"text"},
"projectId": {"type":"text"},
"projectURL": {"type":"text"},
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
host =  '149.165.170.158:80'
#file_location = 'test.csv'
file_location = 'data/amphibian_disease_data_processed.csv'

loader = ESLoader(file_location, index, drop_existing, alias, host)
loader.load()


