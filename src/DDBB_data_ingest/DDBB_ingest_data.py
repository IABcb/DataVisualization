import sys
import os
import pymongo
import csv
import pandas as pd
path_to_append = os.path.dirname(os.path.abspath(__file__)).replace("/DDBB_data_ingest", "")
sys.path.append(path_to_append)
from mongoDBclass.mongoDBclass import mongo as MGDB

def remove_data(mongoOBJ, collection):
    print('TYPE: ' + type)
    mongoOBJ.rm_all_docs_collection(collection)
    mongoOBJ.count_collection_docs(collection)

def insert_data_intoMONGO(mongoOBJ, CSVinput_data, collection, type, sources, coins):
    docs = CSVinput_data.to_dict(orient='records')
    index_docs = list()
    for d in docs:
        date = d['Date']
        data_st = mongoOBJ.create_data_structure(sources, coins, type, date)
        for key_source in sources.keys():
            data_st['Data'][key_source]['Value'] = d[key_source]
        index_docs.append(data_st)

    collection.insert_many(index_docs)
    print('TYPE: ' + type)
    mongoOBJ.count_collection_docs(collection)


if __name__ == "__main__":

    # Mongo Docker Set up
    ddbb_data_path = os.path.dirname(os.path.abspath(__file__)).replace('src/DDBB_data_ingest', 'docker/MongoDB/data')
    collections_list = ['stockExchange', 'unemployment']
    mongoOBJ = MGDB(collections_list, ddbb_data_path)
    collections = mongoOBJ.get_collections()
    # Stock Exchange Data
    sources = {'IBEX35': 'Spain',
               'DJI': 'EEUU',
               'LSE': 'London',
               'N225': 'Japan'}

    coins = {'IBEX35': 'Euros',
             'DJI': 'Dollars',
             'LSE': 'Pounds',
             'N225': 'Yens'}

    # Read Mixed CSVS
    sep = ','
    columnames = ['Date', 'DJI', 'LSE', 'IBEX35', 'N225']

    stockExchangePath = os.path.dirname(os.path.abspath(__file__)).replace('src/DDBB_data_ingest', 'data/datos_bolsa/processed/csv_stockExchange_mixed')
    unemploymentPath  = os.path.dirname(os.path.abspath(__file__)).replace('src/DDBB_data_ingest', 'data/datos_paro/processed/csv_Unem_mixed')

    stockExchangeData = pd.read_csv(stockExchangePath, sep=sep, names=columnames, header = 0)
    unemploymentData = pd.read_csv(unemploymentPath, sep=sep, names=columnames, header = 0)

    # Insert CSV Stock Exchange to mongo
    type = 'stockExchange'
    insert_data_intoMONGO(mongoOBJ, stockExchangeData, collections['stockExchange'], type, sources, coins)

    # Insert CSV Unemployment to mongo
    type = 'unemployment'
    insert_data_intoMONGO(mongoOBJ, unemploymentData, collections['unemployment'], type, sources, coins)

    # Remove data, if needed
    # remove_data(mongoOBJ, collections['stockExchange'])
    # remove_data(mongoOBJ, collections['unemployment'])

    mongoOBJ.close_mongo_conex()
    mongoOBJ.stop_rm_docker_mongo()