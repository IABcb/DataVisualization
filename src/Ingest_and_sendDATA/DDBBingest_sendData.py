import sys
import os
import pymongo
import csv
import time
import pandas as pd
path_to_append = os.path.dirname(os.path.abspath(__file__)).replace("/Ingest_and_sendDATA", "")
sys.path.append(path_to_append)
from mongoDBclass.mongoDBclass import mongo as MGDB
from KafkaConnection.kafka_connection import KafkaConnection as KFK

def init_kafka_docker():
    print('Starting Kafka Broker...')
    curr_path = os.path.dirname(os.path.abspath(__file__)).replace("/src/Ingest_and_sendDATA", "/docker/Spotify_Kafka/")
    run_file = 'run.py'
    os.system('python ' + curr_path + run_file)
    time.sleep(5)

def stop_kafka():
    os.system('docker stop kafka; docker rm kafka')
    time.sleep(2)

def make_query(collection, index):
    print('New query')
    response = collection.find_one({'Date': index})
    return response, finish_queries

def send_data2kafka(data_2send, producer):
    print('Sending data...')
    producer.produce(bytes(data_2send))

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

def update_stock_index(curr_stock_index, finish_stock_index, finish_queries):

    if curr_stock_index == finish_stock_index:
        finish_queries = True
    else:
        # Stock index
        if curr_stock_index.split('-')[1] == '12':
            month = '01'
            year = str(int(curr_stock_index.split('-')[0]) + 1)
        else:
            month = str(int(curr_stock_index.split('-')[1]) + 1)
            year = curr_stock_index.split('-')[0]
            if len(month) == 1:
                month = '0' + month

        curr_stock_index = str(year) + '-' + str(month)

    return curr_stock_index, finish_queries

def update_unemployment_index(curr_unem_index, finish_unem_index, finish_queries):

    if  curr_unem_index == finish_unem_index:
        finish_queries = True
    else:
        if curr_unem_index.split('Q')[1] == '4':
            month = 1
            year = str(int(curr_unem_index.split('Q')[0]) + 1)
        else:
            month = str(int(curr_unem_index.split('Q')[1]) + 1)
            year = curr_unem_index.split('Q')[0]

        curr_unem_index = str(year) + 'Q' + str(month)

    return curr_unem_index, finish_queries

if __name__ == "__main__":

    # Mongo Docker Set up
    ddbb_data_path = os.path.dirname(os.path.abspath(__file__)).replace('src/Ingest_and_sendDATA', 'docker/MongoDB/data')
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

    # Kafka config
    kafka_ip = 'localhost'
    kafka_port = 9092

    # Query parameters
    time_to_query = 3

    if len(sys.argv) == 2:
        if sys.argv[1] == 'ingest':
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

        elif sys.argv[1] == 'remove':
            # Remove data, if needed
            remove_data(mongoOBJ, collections['stockExchange'])
            remove_data(mongoOBJ, collections['unemployment'])

    #  Send data
    try:

        # Kafka
        # init_kafka_docker()
        kafkaObj_stockExchange = KFK(host = kafka_ip, port = kafka_port, topic = 'stockExchange')
        producer_stock = kafkaObj_stockExchange.init_Kafka_producer()

        kafkaObj_unemployment = KFK(host = kafka_ip, port = kafka_port, topic = 'unemployment')
        producer_unem = kafkaObj_unemployment.init_Kafka_producer()

        init_stock_index = '2000-01'
        init_unem_index = '2000Q1'

        finish_stock_index = '2016-11'
        finish_unem_index = '2016Q4'

        qmonths = ['03', '06', '09', '12']

        curr_stock_index = init_stock_index
        curr_unem_index = init_unem_index
        finish_queries = False
        response_stock = None
        response_unem = None
        while not finish_queries:
            # Query maker
            response_stock, finish_queries_stock = \
                make_query(collections['stockExchange'], curr_stock_index)

            # Send extracted data
            send_data2kafka(response_stock, producer_stock)

            print('Response from stock exchange')
            print(response_stock)

            if curr_stock_index.split('-')[1] in qmonths or curr_stock_index == finish_stock_index:
                response_unem, finish_queries_unem = \
                    make_query(collections['unemployment'], curr_unem_index)

                curr_unem_index, finish_queries = \
                    update_unemployment_index(curr_unem_index,
                                finish_unem_index, finish_queries)

                # Send extracted data
                send_data2kafka(response_unem, producer_unem)
                print('Response from unemployment')
                print(response_unem)

            curr_stock_index, finish_queries = \
                update_stock_index(curr_stock_index, finish_stock_index, finish_queries)

            # Time to next query
            time.sleep(time_to_query)

        stop_kafka()
        mongoOBJ.close_mongo_conex()
        mongoOBJ.stop_rm_docker_mongo()
    except KeyboardInterrupt:
        stop_kafka()
        mongoOBJ.close_mongo_conex()
        mongoOBJ.stop_rm_docker_mongo()

