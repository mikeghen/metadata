from datetime import datetime
from pprint import pprint
import pandas as pd
import numpy as np
import urllib
import json
import os
import math
from google.cloud import bigquery
import multiprocessing as mp
from http.client import IncompleteRead
import ssl
context = ssl._create_unverified_context()



os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./key.json"
URI = 'https://openpaymentsdata.cms.gov'
UUID = 'kf8w-ky3z'
CLIENT = bigquery.Client()
DATASET_ID = 'opendata'

def main():
    """
    Extract and load all the data from open data portal
    """
    url = 'https://openpaymentsdata.cms.gov/views/'
    response = urllib.request.urlopen(url, context=context)
    datasets  = json.load(response)
    # outfile = open('metadata.json','w')
    for dataset in datasets:
        _process_dataset(dataset)


def _process_dataset(dataset):
    if not dataset['id'] in ('ixmm-mia4','xuhg-sf8r','aznh-ka38','mm4s-i2yf','rzgi-r2q6','98zw-w39y','jgfz-8vvz','7vc6-qhcg','pere-mq5r','tgv9-2kjv','3rhv-ge3t','ap6w-xznw','dxxx-2p5m','5gdd-kf9d','4j6e-tv7h'):
        extracted_data = _extract(dataset['id'])
        # print("{0} has {1} records...".format(extracted_data['tablename'],len(extracted_data['data'])))
        _load_data(dataset['id'],extracted_data['tablename'],extracted_data['path'])


def _load_data(uuid, table_id, data_file_path):
    """
    Loads data into BigQuery
    https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/usage.html#tables
    data_file_path is a path to a JSON file containing the data set's data
    uuid is the data sets's uuid
    """
    dataset_ref = CLIENT.dataset(DATASET_ID)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = _construct_schema(uuid)

    with open(data_file_path, 'rb') as source_file:
        job = CLIENT.load_table_from_file(
            source_file,
            table_ref,
            location='US',  # Must match the destination dataset location.
            job_config=job_config)  # API request

    job.result()  # Waits for table load to complete.

    print('Loaded {} rows into {}:{}.'.format(job.output_rows, DATASET_ID, table_id))


def _construct_schema(uuid):
    """
    Creates a BigQuery Schema for the data set with uuid.
    https://cloud.google.com/bigquery/docs/schemas
    https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.schema.SchemaField.html
    https://socratadiscovery.docs.apiary.io/#reference/0/find-by-id/search-by-id
    """
    catalog_url = '{0}/api/catalog/v1?ids={1}'.format(URI, uuid)
    response = urllib.request.urlopen(catalog_url, context=context)
    catalog_data  = json.load(response)["results"][0]["resource"]

    schema = []
    for i in range(0, len(catalog_data["columns_field_name"])):
        name = catalog_data["columns_field_name"][i]
        field_type = _encode_datatype(catalog_data["columns_datatype"][i])
        description = catalog_data["columns_description"][i]
        schema.append(bigquery.SchemaField(name, field_type, mode='NULLABLE', description=description))

    return schema


def _encode_datatype(datatype):
    """
    Converts open data data types to BigQuery datatypes
    https://dev.socrata.com/docs/datatypes/
    BigQuery Types: ‘STRING’, ‘INTEGER’, ‘FLOAT’, ‘NUMERIC’,
                    ‘BOOLEAN’, ‘TIMESTAMP’ or ‘RECORD’
    """
    types = {
        'Text': 'STRING',
        'Number': 'NUMERIC',
        'Double': 'NUMERIC',
        'Calendar date': 'TIMESTAMP'
    }
    return types[datatype]


def _make_tablename(name):
    tablename = ''
    for char in name[0:128]:
        if 'z' >= char >= 'a' or 'Z' >= char >= 'A' or '9' >= char >= '0':
            tablename += char
        else:
            tablename += '_'
    return tablename


def _download_data_job_processor(data_url):
    path = 'data.json'
    print("Processing URL {0}".format(data_url))

    """
    Safely attempt to download the data from the data_url, try until successful
    """
    successful = False
    errors = 0
    while not successful and errors < 20:
        try:
            response = urllib.request.urlopen(data_url, context=context)
            data = json.load(response)
            successful = True
        except (IncompleteRead,json.decoder.JSONDecodeError, urllib.error.URLError) as e:
            print("Error downloading data from {0}".format(data_url))
            print(e)
            successful = False
            errors += 1
        except:
            print("Unknown error downloading data from {0}".format(data_url))
            successful = False
            errors += 1

    data_file = open(path, 'a')
    for row in data:
        data_file.write(json.dumps(row) + '\n')
    data_file.close()
    print("Wrote {0} records to {1} from {2}".format(len(data), path, data_url))


def _extract(uuid):
    """
    Extracts data set from an open data portal.
    """
    metadata_url = '{0}/api/views/metadata/v1/{1}'.format(URI, uuid)
    response = urllib.request.urlopen(metadata_url, context=context)
    metadata  = json.load(response)
    metadata['createdAt'] = metadata['createdAt'].replace('T',' ').replace('+0000','')
    metadata['dataUpdatedAt'] = metadata['dataUpdatedAt'].replace('T',' ').replace('+0000','')
    metadata['metadataUpdatedAt'] = metadata['metadataUpdatedAt'].replace('T',' ').replace('+0000','')
    metadata['updatedAt'] = metadata['updatedAt'].replace('T',' ').replace('+0000','')
    keys = list(metadata['customFields'].keys())
    for key in keys:
        new_key = key.replace(' ', '_')
        metadata['customFields'][new_key] = metadata['customFields'][key]
        _keys = list(metadata['customFields'][key].keys())
        for _key in _keys:
            _new_key = _key.replace(' ', '_')
            metadata['customFields'][new_key][_new_key] = metadata['customFields'][key][_key]
            del metadata['customFields'][key][_key]
        del metadata['customFields'][key]

    # Page through all data 50000 at a time
    datasize_url = '{0}/resource/{1}.json?$select=count(*)'.format(URI, uuid)
    response = urllib.request.urlopen(datasize_url, context=context)
    data_size  = json.load(response)
    data_size = int(data_size[0]["count"])
    data_copied = 0
    tablename = _make_tablename(metadata['name'])
    print(tablename)
    path = tablename + '.json'
    data_urls = []
    while data_copied < data_size:
        data_url = metadata['dataUri'] + '.json?$limit=50000&$offset={0}'.format(data_copied)
        data_urls.append(data_url)
        data_copied += 50000
    # Clear the file before we start:
    path = 'data.json'
    open(path, 'w').close()
    pool = mp.Pool(processes=8)
    results = pool.map(_download_data_job_processor, data_urls)


    return {'metadata': metadata, 'tablename': tablename, 'path': 'data.json' }
