from pymongo import MongoClient
import json
import os
import sys
import time
import csv
import pandas as pd

class mongo():
    def __init__(self, collections_list, ddbb_data_path, ddbb = 'visualization'):
        self.ddbb_data_path = ddbb_data_path
        os.system('sudo docker run --rm --name my-mongo -v ' +  self.ddbb_data_path + ':/data/db -it -d -p 27017:27017 mongo:latest')
        self.client = MongoClient()
        self.collections_list = collections_list
        self.db = self.client.ddbb
        self.collections = dict()

        for col in self.collections_list:
            self.collections[col] = self.db.col

    def get_collections(self):
        return self.collections

    def insert_doc(self, collection, doc):
        print('Inserting doc')
        collection.insert(doc)

    def check_collection_docs(self, collection):
        for i in collection.find():
            print('The doc is: ')
            print(i)

    def count_collection_docs(self, collection):
        print('Items of collection')
        print(collection.count())

    def stop_rm_docker_mongo(self):
        os.system('sudo docker stop my-mongo; sudo docker rm my-mongo')

    def close_mongo_conex(self):
        self.client.close()

    def rm_doc_from_collection(self, collection, doc):
        collection.delete_one(doc)

    def rm_all_docs_collection(self, collection):
        collection.delete_many({})

    def rm_collection(self, collection):
        collection.drop()

    def create_data_structure(self, sources, coins, type, date):
        message_structure = dict()
        data_structure = dict()

        message_structure['Type'] = type
        message_structure['Date'] = date

        for stockExchange in coins.keys():
            data_structure[stockExchange] = dict()
            data_structure[stockExchange]['Coin'] = coins[stockExchange]
            data_structure[stockExchange]['Source'] = sources[stockExchange]
            data_structure[stockExchange]['Value'] = 'NA'

        message_structure['Data'] = data_structure

        return message_structure


class IBEX35():
    def __init__(self, input_stock_path, output_stock_path, input_unem_path, output_unem_pat, stock_column_names):
        self.input_stock_path = input_stock_path
        self.output_stock_path = output_stock_path
        self.input_unem_path = input_unem_path
        self.output_unem_pat = output_unem_pat
        self.stock_column_names = stock_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names)

        self.output_stock_file = open(self.output_stock_path, 'w')
        self.output_stock_writer = csv.writer(self.output_stock_file, delimiter=self.sep, quotechar='"')

    def process_unemploymentData(self):
        # self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep, names=)

        self.output_unem_file = open(self.output_unem_pat, 'w')
        self.output_unem_writer = csv.writer(self.output_unem_file, delimiter=self.sep, quotechar='"')

class DJI():
    def __init__(self, input_stock_path, output_stock_path, input_unem_path, output_unem_pat, stock_column_names):
        self.input_stock_path = input_stock_path
        self.output_stock_path = output_stock_path
        self.input_unem_path = input_unem_path
        self.output_unem_pat = output_unem_pat
        self.stock_column_names = stock_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names)

        self.output_stock_file = open(self.output_stock_path, 'w')
        self.output_stock_writer = csv.writer(self.output_stock_file, delimiter=self.sep, quotechar='"')

    def process_unemploymentData(self):
        # self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep, names=)

        self.output_unem_file = open(self.output_unem_pat, 'w')
        self.output_unem_writer = csv.writer(self.output_unem_file, delimiter=self.sep, quotechar='"')

class LSE():
    def __init__(self, input_stock_path, output_stock_path, input_unem_path, output_unem_pat, stock_column_names):
        self.input_stock_path = input_stock_path
        self.output_stock_path = output_stock_path
        self.input_unem_path = input_unem_path
        self.output_unem_pat = output_unem_pat
        self.stock_column_names = stock_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names)

        self.output_stock_file = open(self.output_stock_path, 'w')
        self.output_stock_writer = csv.writer(self.output_stock_file, delimiter=self.sep, quotechar='"')

    def process_unemploymentData(self):
        # self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep, names=)

        self.output_unem_file = open(self.output_unem_pat, 'w')
        self.output_unem_writer = csv.writer(self.output_unem_file, delimiter=self.sep, quotechar='"')

class N255():
    def __init__(self, input_stock_path, output_stock_path, input_unem_path, output_unem_pat, stock_column_names):
        self.input_stock_path = input_stock_path
        self.output_stock_path = output_stock_path
        self.input_unem_path = input_unem_path
        self.output_unem_pat = output_unem_pat
        self.stock_column_names = stock_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names)

        self.output_stock_file = open(self.output_stock_path, 'w')
        self.output_stock_writer = csv.writer(self.output_stock_file, delimiter=self.sep, quotechar='"')

    def process_unemploymentData(self):
        # self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep, names=)

        self.output_unem_file = open(self.output_unem_pat, 'w')
        self.output_unem_writer = csv.writer(self.output_unem_file, delimiter=self.sep, quotechar='"')

if __name__ == "__main__":
    # Stock Exchange Data
    sources = {'IBEX35': 'Spain',
                 'DJI': 'EEUU',
                 'LSE': 'London',
                 'N255': 'Japan'}

    coins = {'IBEX35': 'Euros',
                 'DJI': 'Dollars',
                 'LSE': 'Pounds',
                 'N255': 'Yens'}

    stock_column_names = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']

    # Paths StockExchange
    stock_general_input = os.path.dirname(os.path.abspath(__file__)).replace('src', 'data/datos_bolsa/')
    stock_general_output = os.path.dirname(os.path.abspath(__file__)).replace('src', 'data/datos_bolsa/processed/')
    stock_inputs = {'IBEX35': stock_general_input + '^IBEX.csv',
                 'DJI': stock_general_input + '^DJI.csv',
                 'LSE': stock_general_input + '^LSE.csv',
                 'N255': stock_general_input + '^N225.csv'}
    stock_outputs = {'IBEX35': stock_general_output + '^IBEX.csv',
                 'DJI': stock_general_output + '^DJI.csv',
                 'LSE': stock_general_output + '^LSE.csv',
                 'N255': stock_general_output + '^N225.csv'}

    # Paths Unemployment
    unem_general_input = os.path.dirname(os.path.abspath(__file__)).replace('src', 'data/datos_paro/')
    unem_general_output = os.path.dirname(os.path.abspath(__file__)).replace('src', 'data/datos_paro/processed/')
    unemployment_inputs = {'IBEX35': unem_general_input + 'espana/4247.csv',
                 'DJI': unem_general_input + 'eeuu/SeriesReport-20170604132227_e518f3.csv',
                 'LSE': unem_general_input + 'london/series-040617.csv',
                 'N255': unem_general_input + 'japon/jma_data.csv'}
    unemployment_outputs = {'IBEX35': unem_general_output + '4247.csv',
                 'DJI': unem_general_output + 'SeriesReport-20170604132227_e518f3.csv',
                 'LSE': unem_general_output + 'series-040617.csv',
                 'N255': unem_general_output + 'jma_data.csv'}


    # Mongo Docker Set up
    # ddbb_data_path = os.path.dirname(os.path.abspath(__file__)).replace('src', 'MongoDB')
    # collections_list = ['stockExchange', 'unemployment']
    # mongoOBJ = mongo(collections_list, ddbb_data_path)
    # collections = mongoOBJ.get_collections()

    # structure = mongoOBJ.create_data_structure(sources, coins, 'stockExchange', '2000/01')
    # mongoOBJ.insert_doc(collections['stockExchange'], structure)
    # mongoOBJ.check_collection_docs(collections['stockExchange'])
    #
    # mongoOBJ.rm_all_docs_collection(collections['stockExchange'])
    # mongoOBJ.check_collection_docs(collections['stockExchange'])


    IBEX35 = IBEX35(stock_inputs['IBEX35'], stock_outputs['IBEX35'], unemployment_inputs['IBEX35'], unemployment_outputs['IBEX35'], stock_column_names)
    DJI = DJI(stock_inputs['DJI'], stock_outputs['DJI'], unemployment_inputs['DJI'], unemployment_outputs['DJI'], stock_column_names)
    LSE = LSE(stock_inputs['LSE'], stock_outputs['LSE'], unemployment_inputs['LSE'], unemployment_outputs['LSE'], stock_column_names)
    N255 = N255(stock_inputs['N255'], stock_outputs['N255'], unemployment_inputs['N255'], unemployment_outputs['N255'], stock_column_names)

    IBEX35.process_stockExchangeData()
    # IBEX35.process_stockExchangeData()
    # DJI.process_stockExchangeData()
    # DJI.process_stockExchangeData()
    # LSE.process_stockExchangeData()
    # LSE.process_stockExchangeData()
    # N255.process_stockExchangeData()
    # N255.process_stockExchangeData()

    # mongoOBJ.close_mongo_conex()
    # mongoOBJ.stop_rm_docker_mongo()