import json
import os
import sys
import time
import csv
import pandas as pd
path_to_append = os.path.dirname(os.path.abspath(__file__)).replace("/process_data", "")
sys.path.append(path_to_append)
from mongoDBclass.mongoDBclass import mongo as MGDB

class IBEX35():
    def __init__(self, input_stock_path, output_stock_path, input_unem_path, output_unem_pat, stock_column_names):
        self.input_stock_path = input_stock_path
        # self.output_stock_path = output_stock_path
        self.input_unem_path = input_unem_path
        # self.output_unem_pat = output_unem_pat
        self.stock_column_names = stock_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        # Read stockExchange data
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names)
        # Open output file
        # self.output_stock_file = open(self.output_stock_path, 'w')
        # self.output_stock_writer = csv.writer(self.output_stock_file, delimiter=self.sep, quotechar='"')

        # Drop first value. Is not common in all dataframes
        self.process_data = self.input_stock_file[self.input_stock_file.Date != '1999-12-31']
        self.process_data = self.process_data.reset_index(drop = True)

        # Change date format
        for date in self.process_data.Date[1:]:
            process_date = '-'.join(date.split('-')[:-1])
            index_row = self.process_data[self.process_data.Date == date].index.tolist()
            self.process_data.set_value(index_row, 'Date', process_date)

        return self.process_data

    def process_unemploymentData(self):
        # self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep, names=)
        # Open output file
        self.output_unem_file = open(self.output_unem_pat, 'w')
        self.output_unem_writer = csv.writer(self.output_unem_file, delimiter=self.sep, quotechar='"')

class DJI():
    def __init__(self, input_stock_path, output_stock_path, input_unem_path, output_unem_pat, stock_column_names):
        self.input_stock_path = input_stock_path
        # self.output_stock_path = output_stock_path
        self.input_unem_path = input_unem_path
        # self.output_unem_pat = output_unem_pat
        self.stock_column_names = stock_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        # Read stockExchange data
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names)
        # Open output file
        # self.output_stock_file = open(self.output_stock_path, 'w')
        # self.output_stock_writer = csv.writer(self.output_stock_file, delimiter=self.sep, quotechar='"')

        # Drop last value. Is not common in all dataframes
        self.process_data = self.input_stock_file[self.input_stock_file.Date != '2016-12-01']
        self.process_data = self.process_data.reset_index(drop = True)

        # Change date format
        for date in self.process_data.Date[1:]:
            process_date = '-'.join(date.split('-')[:-1])
            index_row = self.process_data[self.process_data.Date == date].index.tolist()
            self.process_data.set_value(index_row, 'Date', process_date)

        return self.process_data

    def process_unemploymentData(self):
        # self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep, names=)
        # Open output file
        self.output_unem_file = open(self.output_unem_pat, 'w')
        self.output_unem_writer = csv.writer(self.output_unem_file, delimiter=self.sep, quotechar='"')

class LSE():
    def __init__(self, input_stock_path, output_stock_path, input_unem_path, output_unem_pat, stock_column_names):
        self.input_stock_path = input_stock_path
        # self.output_stock_path = output_stock_path
        self.input_unem_path = input_unem_path
        # self.output_unem_pat = output_unem_pat
        self.stock_column_names = stock_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        # Read stockExchange data
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names)

        # Open output file
        # self.output_stock_file = open(self.output_stock_path, 'w')
        # self.output_stock_writer = csv.writer(self.output_stock_file, delimiter=self.sep, quotechar='"')

        # Drop last value. Is not common in all dataframes
        self.process_data = self.input_stock_file[self.input_stock_file.Date != '2016-12-01']
        self.process_data = self.process_data.reset_index(drop = True)

        # Add non existing month for each year
        month_to_add = '10'
        prev_month = '09'
        fill = None
        for date in self.process_data.Date[1:]:
            if date.split('-')[1] == prev_month:
                curr_index = self.process_data[self.process_data['Date'] == date].index.tolist()
                curr_index = curr_index[0]
                next_index = curr_index + 1

                # Create new date
                date_to_insert = date.split('-')
                date_to_insert[1] = month_to_add
                date_to_insert = '-'.join(date_to_insert)

                # Create new row
                line = [date_to_insert, fill, fill, fill, fill, fill, fill]
                line = pd.DataFrame.from_records([line], columns= self.stock_column_names, index= [next_index])

                # Insert row in dataframe
                last_df = self.process_data[next_index:]
                last_df.index = last_df.index + 1
                frames = [self.process_data[:next_index],
                        line,
                        last_df]
                self.process_data = pd.concat(frames)

        # Remove repeated 03 month for each year, from 2002
        init_date_remove = '2002-03'
        month_remove = '03'
        next_date_remove = init_date_remove
        for date in self.process_data.Date:
            if '-'.join(date.split('-')[:2]) == next_date_remove:
                self.process_data = self.process_data[self.process_data.Date != date]
                self.process_data = self.process_data.reset_index(drop=True)
                next_date_remove = str(int(next_date_remove.split('-')[0]) + 1) + '-' + month_remove

        # Drop column names value. For easier filling dates.
        self.process_data = self.process_data[1:]

        # Fill with None from 2000-01-01 until 2001-07-31, for common structure
        init_date = '2001-06-31'
        last_date = '1999-12-31'
        fill = None

        next_date = init_date
        while next_date != last_date:
            # adding a row
            self.process_data.loc[-1] = [next_date, fill, fill, fill, fill, fill, fill]
            # shifting index
            self.process_data.index = self.process_data.index + 1
            # sorting by index
            self.process_data = self.process_data.sort_index()

            next_date = next_date.split('-')

            next_month = '0' + str(int(next_date[1]) - 1)
            if len(next_month) == 3:
                next_month = next_month[1:]

            if int(next_month) >= 1:
                next_date[1] = next_month
            else:
                next_month = '12'
                next_year = str(int(next_date[0]) - 1)
                next_date[0] = next_year
                next_date[1] = next_month
            next_date = '-'.join(next_date)

        # adding a columnames again
        self.process_data.loc[-1] = self.stock_column_names
        # shifting index
        self.process_data.index = self.process_data.index + 1
        # sorting by index
        self.process_data = self.process_data.sort_index()

        # Change date format
        for date in self.process_data.Date[1:]:
            process_date = '-'.join(date.split('-')[:-1])
            index_row = self.process_data[self.process_data.Date == date].index.tolist()
            self.process_data.set_value(index_row, 'Date', process_date)

        return self.process_data

    def process_unemploymentData(self):
        # self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep, names=)
        # Open output file
        self.output_unem_file = open(self.output_unem_pat, 'w')
        self.output_unem_writer = csv.writer(self.output_unem_file, delimiter=self.sep, quotechar='"')

class N255():
    def __init__(self, input_stock_path, output_stock_path, input_unem_path, output_unem_pat, stock_column_names):
        self.input_stock_path = input_stock_path
        # self.output_stock_path = output_stock_path
        self.input_unem_path = input_unem_path
        # self.output_unem_pat = output_unem_pat
        self.stock_column_names = stock_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        # Read stockExchange data
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names)
        # Open output file
        # self.output_stock_file = open(self.output_stock_path, 'w')
        # self.output_stock_writer = csv.writer(self.output_stock_file, delimiter=self.sep, quotechar='"')

        self.process_data = self.input_stock_file

        # Change date format
        for date in self.process_data.Date[1:]:
            process_date = '-'.join(date.split('-')[:-1])
            index_row = self.process_data[self.process_data.Date == date].index.tolist()
            self.process_data.set_value(index_row, 'Date', process_date)

        return self.process_data

    def process_unemploymentData(self):
        # self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep, names=)
        # Open output file
        self.output_unem_file = open(self.output_unem_pat, 'w')
        self.output_unem_writer = csv.writer(self.output_unem_file, delimiter=self.sep, quotechar='"')

def mix_csvs(dict_csvs, column_names, index_colum, value_to_extract, output_path, sep = ','):
    csv_file = open(output_path, 'w')
    csv_writer = csv.writer(csv_file, delimiter=sep, quotechar='"')

    keys = dict_csvs.keys()
    first_csv = dict_csvs[keys[0]]

    csv_writer.writerow(column_names)
    for value in first_csv[first_csv.columns[0]][1:]:
        row = list()
        row.append(value)
        for key in dict_csvs.keys():
            value_to_append = dict_csvs[key][value_to_extract][dict_csvs[key][index_colum] == value].item()
            row.append(value_to_append)
        csv_writer.writerow(row)

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
    stock_general_input = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_bolsa/')
    stock_general_output = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_bolsa/processed/')
    stock_inputs = {'IBEX35': stock_general_input + '^IBEX.csv',
                 'DJI': stock_general_input + '^DJI.csv',
                 'LSE': stock_general_input + '^LSE.csv',
                 'N225': stock_general_input + '^N225.csv'}
    stock_outputs = {'IBEX35': stock_general_output + '^IBEX.csv',
                 'DJI': stock_general_output + '^DJI.csv',
                 'LSE': stock_general_output + '^LSE.csv',
                 'N225': stock_general_output + '^N225.csv'}

    # Paths Unemployment
    unem_general_input = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_paro/')
    unem_general_output = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_paro/processed/')
    unemployment_inputs = {'IBEX35': unem_general_input + 'espana/4247.csv',
                 'DJI': unem_general_input + 'eeuu/SeriesReport-20170604132227_e518f3.csv',
                 'LSE': unem_general_input + 'london/series-040617.csv',
                 'N225': unem_general_input + 'japon/jma_data.csv'}
    unemployment_outputs = {'IBEX35': unem_general_output + '4247.csv',
                 'DJI': unem_general_output + 'SeriesReport-20170604132227_e518f3.csv',
                 'LSE': unem_general_output + 'series-040617.csv',
                 'N225': unem_general_output + 'jma_data.csv'}


    # Mongo Docker Set up
    # ddbb_data_path = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'docker/MongoDB/data')
    # collections_list = ['stockExchange', 'unemployment']
    # mongoOBJ = MGDB(collections_list, ddbb_data_path)
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
    N255 = N255(stock_inputs['N225'], stock_outputs['N225'], unemployment_inputs['N225'], unemployment_outputs['N225'], stock_column_names)

    stocks_dict = dict()
    stocks_dict['IBEX35'] = IBEX35.process_stockExchangeData()
    stocks_dict['DJI'] = DJI.process_stockExchangeData()
    stocks_dict['LSE'] = LSE.process_stockExchangeData()
    stocks_dict['N225'] = N255.process_stockExchangeData()

    column_names = ['Date'] + stocks_dict.keys()
    value_to_extract = 'Close'
    index_colum = 'Date'
    output_path = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_bolsa/processed/')
    filename = 'csv_stockExchange_mixed'
    output_path = output_path + filename

    mix_csvs(stocks_dict, column_names, index_colum, value_to_extract, output_path)

    # IBEX35.process_unemploymentData()
    # DJI.process_unemploymentData()
    # LSE.process_unemploymentData()
    # N255.process_unemploymentData()

    # mongoOBJ.close_mongo_conex()
    # mongoOBJ.stop_rm_docker_mongo()
