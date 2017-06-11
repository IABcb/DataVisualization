import json
import os
import sys
import time
import csv
import pandas as pd

class IBEX35():
    def __init__(self, input_stock_path, input_unem_path, stock_column_names, unem_column_names):
        self.input_stock_path = input_stock_path
        self.input_unem_path = input_unem_path
        self.stock_column_names = stock_column_names
        self.unem_column_names = unem_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        # Read stockExchange data
        print('Reading IBEX35 stock Exchange data')
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names, header = 0)
        print(' Processing...')

        # Drop first value. Is not common in all dataframes
        self.process_data = self.input_stock_file[self.input_stock_file.Date != '1999-12-31']
        self.process_data = self.process_data.reset_index(drop = True)

        # Change date format
        for date in self.process_data.Date:
            process_date = '-'.join(date.split('-')[:-1])
            index_row = self.process_data[self.process_data.Date == date].index.tolist()
            self.process_data.set_value(index_row, 'Date', process_date)
        print(' Done!')
        return self.process_data

    def process_unemploymentData(self):
        # Read unemployment data
        print('Reading IBEX35 unemployment data')
        self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep, names=self.unem_column_names, header = 0)
        self.process_data_unem = self.input_unem_file


        print(' Processing...')
        # Transpose the dataframe
        self.process_data_unem = self.process_data_unem.transpose()
        self.process_data_unem['Date'] = self.process_data_unem.index
        # Rename columns
        self.process_data_unem = self.process_data_unem.rename(columns={0: "Percentage"})
        # Sort by date
        self.process_data_unem = self.process_data_unem.sort_values(by='Date')
        self.process_data_unem = self.process_data_unem[:-1]
        self.process_data_unem = self.process_data_unem.reset_index(drop=True)
        sequence = ['Date', 'Percentage']
        self.process_data_unem = self.process_data_unem.reindex(columns=sequence)

        # Fill data from 2000
        start_date = '2001T4'
        stop_date = '1999T4'
        next_date = start_date
        fill = 'None'
        indicator = 'T'
        next_indicator = 4
        for date in self.process_data_unem.Date:
            if next_date != stop_date:
                self.process_data_unem.loc[-1] = [next_date, fill]
                # shifting index
                self.process_data_unem.index = self.process_data_unem.index + 1
                # sorting by index
                self.process_data_unem = self.process_data_unem.sort_index()

                if next_date.split(indicator)[1] == '1':
                    next_indicator = 4
                    next_date = str(int(next_date.split(indicator)[0]) - 1) + indicator + str(next_indicator)
                else:
                    next_indicator = next_indicator - 1
                    next_date = str(int(next_date.split(indicator)[0])) + indicator + str(next_indicator)
            else:
                break

        # Change T idicator for Q
        change_indicator = 'Q'
        for date in self.process_data_unem.Date:
            date_input = date.split(indicator)[0] + change_indicator + date.split(indicator)[1]
            index_row = self.process_data_unem[self.process_data_unem.Date == date].index.tolist()
            self.process_data_unem.set_value(index_row, 'Date', date_input)
        print(' Done!')
        self.process_data_unem = self.process_data_unem.reset_index(drop=True)
        return self.process_data_unem

class DJI():
    def __init__(self, input_stock_path, input_unem_path, stock_column_names, unem_column_names):
        self.input_stock_path = input_stock_path
        self.input_unem_path = input_unem_path
        self.stock_column_names = stock_column_names
        self.unem_column_names = unem_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        # Read stockExchange data
        print('Reading DJI stock Exchange data')
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names, header = 0)
        print(' Processing...')

        # Drop last value. Is not common in all dataframes
        self.process_data = self.input_stock_file[self.input_stock_file.Date != '2016-12-01']
        self.process_data = self.process_data.reset_index(drop = True)

        # Change date format
        for date in self.process_data.Date:
            process_date = '-'.join(date.split('-')[:-1])
            index_row = self.process_data[self.process_data.Date == date].index.tolist()
            self.process_data.set_value(index_row, 'Date', process_date)
        print(' Done!')
        return self.process_data

    def process_unemploymentData(self):
        # Read unemployment data
        print('Reading DJI unemployment data')
        self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep, names=self.unem_column_names, header = 0)

        print(' Processing...')

        nQs = 4
        curr_Q = 1
        indicator = 'Q'
        next_index = 0
        Q_months = ['None', 'Mar', 'Jun', 'Sep', 'Dec']
        cols = ['Date', 'Percentage']
        init = ['None', 'None']

        self.process_data_unem = pd.DataFrame.from_records([init], columns=cols, index=[next_index])
        self.process_data_unem = self.process_data_unem.reset_index(drop=True)
        for date in self.input_unem_file.Year:
            for d in range(nQs):
                next_index += 1
                row = list()
                curr_date = str(date) + indicator + str(curr_Q)
                row.append(curr_date)
                row.append(self.input_unem_file[Q_months[curr_Q]][self.input_unem_file.Year == date].item())

                line = pd.DataFrame.from_records([row], columns=cols, index= [next_index])
                frames = [self.process_data_unem, line]
                self.process_data_unem = pd.concat(frames)

                if curr_Q == nQs:
                    curr_Q = 1
                else:
                    curr_Q += 1

        self.process_data_unem = self.process_data_unem[1:]
        self.process_data_unem.index = self.process_data_unem.index - 1
        print(' Done!')
        return self.process_data_unem


class LSE():
    def __init__(self, input_stock_path, input_unem_path, stock_column_names, unem_column_names):
        self.input_stock_path = input_stock_path
        self.input_unem_path = input_unem_path
        self.stock_column_names = stock_column_names
        self.unem_column_names = unem_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        # Read stockExchange data
        print('Reading LSE stock Exchange data')
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names, header = 0)
        print(' Processing...')

        # Drop last value. Is not common in all dataframes
        self.process_data = self.input_stock_file[self.input_stock_file.Date != '2016-12-01']
        self.process_data = self.process_data.reset_index(drop = True)

        # Add non existing month for each year
        month_to_add = '10'
        prev_month = '09'
        fill = 'None'
        for date in self.process_data.Date:
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

        # Fill with None from 2000-01-01 until 2001-07-31, for common structure
        init_date = '2001-06-31'
        last_date = '1999-12-31'
        fill = 'None'

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

        # Change date format
        for date in self.process_data.Date:
            process_date = '-'.join(date.split('-')[:-1])
            index_row = self.process_data[self.process_data.Date == date].index.tolist()
            self.process_data.set_value(index_row, 'Date', process_date)
        print(' Done!')
        return self.process_data

    def process_unemploymentData(self):
        # Read unemployment data
        print('Reading LSE unemployment data')
        self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep)
        self.process_data_unem = self.input_unem_file

        print(' Processing...')
        # Rename column names
        self.process_data_unem = self.process_data_unem.rename(columns={'1971': "Date", '4.1': "Percentage"})

        # Extract info with interesting data
        init_date = '2000 Q1'
        end_date = '2016 Q4'
        start_index = self.process_data_unem[self.process_data_unem.Date == init_date].index.tolist()[0]
        end_index = self.process_data_unem[self.process_data_unem.Date == end_date].index.tolist()[0]
        self.process_data_unem = self.process_data_unem[start_index:end_index+1]
        self.process_data_unem = self.process_data_unem.reset_index(drop=True)

        # Change date format
        for date in self.process_data_unem.Date:
            date_toinsert = ''.join(date.split(' '))
            index_row = self.process_data_unem[self.process_data_unem.Date == date].index.tolist()
            self.process_data_unem.set_value(index_row, 'Date', date_toinsert)

        print(' Done!')
        return self.process_data_unem

class N225():
    def __init__(self, input_stock_path, input_unem_path, stock_column_names, unem_column_names):
        self.input_stock_path = input_stock_path
        self.input_unem_path = input_unem_path
        self.stock_column_names = stock_column_names
        self.unem_column_names = unem_column_names
        self.sep = ','

    def process_stockExchangeData(self):
        # Read stockExchange data
        print('Reading N225 stock Exchange data')
        self.input_stock_file = pd.read_csv(self.input_stock_path, sep=self.sep, names=self.stock_column_names, header = 0)

        self.process_data = self.input_stock_file
        print(' Processing...')
        # Change date format
        for date in self.process_data.Date:
            process_date = '-'.join(date.split('-')[:-1])
            index_row = self.process_data[self.process_data.Date == date].index.tolist()
            self.process_data.set_value(index_row, 'Date', process_date)
        print(' Done!')
        return self.process_data

    def process_unemploymentData(self):
        # Read unemployment data
        print('Reading N225 unemployment data')
        self.input_unem_file = pd.read_csv(self.input_unem_path, sep=self.sep)
        self.process_data_unem = self.input_unem_file

        print(' Processing...')
        # Rename column names
        self.process_data_unem = self.process_data_unem.rename(columns={'1963/01': "Date", '1.4': "Percentage"})

        # Extract info with interesting data
        init_date = '2000/01'
        end_date = '2016/12'
        start_index = self.process_data_unem[self.process_data_unem.Date == init_date].index.tolist()[0]
        end_index = self.process_data_unem[self.process_data_unem.Date == end_date].index.tolist()[0]
        self.process_data_unem = self.process_data_unem[start_index:end_index+1]
        self.process_data_unem = self.process_data_unem.reset_index(drop=True)

        # Extract Qs per year
        interesting_months = ['03', '06', '09', '12']
        indexes_to_drop = list()
        for date in self.process_data_unem.Date:
            if date.split('/')[1] not in interesting_months:
                index_to_drop = self.process_data_unem[self.process_data_unem.Date == date].index.tolist()[0]
                indexes_to_drop.append(index_to_drop)
        self.process_data_unem.drop(self.process_data_unem.index[indexes_to_drop], inplace=True)
        self.process_data_unem = self.process_data_unem.reset_index(drop=True)

        # Change indicator
        indicator = 'Q'
        init_indicator = '/'

        for date in self.process_data_unem.Date:
            date_insert = date.split(init_indicator)[0] + indicator + str(int(date.split(init_indicator)[1])/3)
            index_row = self.process_data_unem[self.process_data_unem.Date == date].index.tolist()[0]
            self.process_data_unem.set_value(index_row, 'Date', date_insert)
        print(' Done!')
        return self.process_data_unem

def toCSV_Processed_data(dict_csvs, column_names, index_colum, value_to_extract, output_path, sep = ','):
    # Open output file
    csv_file = open(output_path, 'w')
    csv_writer = csv.writer(csv_file, delimiter=sep, quotechar='"')

    # Get first dataframe for iteration of data
    keys = dict_csvs.keys()
    first_csv = dict_csvs[keys[0]]

    # Write header of csv output file
    csv_writer.writerow(column_names)
    for value in first_csv[first_csv.columns[0]]:
        row = list()
        row.append(value)
        for key in dict_csvs.keys():
            value_to_append = dict_csvs[key][value_to_extract][dict_csvs[key][index_colum] == value].item()
            if value_to_append is None:
                print(key, value)

            row.append(value_to_append)
        csv_writer.writerow(row)

if __name__ == "__main__":

    stock_column_names = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    IBEX35_unem_column_names = ['Date', '2016T4', '2016T3', '2016T2', '2016T1',
                                '2015T4', '2015T3',	'2015T2', '2015T1',
                                '2014T4', '2014T3',	'2014T2', '2014T1',
                                '2013T4', '2013T3',	'2013T2', '2013T1',
                                '2012T4', '2012T3',	'2012T2', '2012T1',
                                '2011T4', '2011T3',	'2011T2', '2011T1',
                                '2010T4', '2010T3',	'2010T2', '2010T1',
                                '2009T4', '2009T3',	'2009T2', '2009T1',
                                '2008T4', '2008T3',	'2008T2', '2008T1',
                                '2007T4', '2007T3',	'2007T2', '2007T1',
                                '2006T4', '2006T3',	'2006T2', '2006T1',
                                '2005T4', '2005T3',	'2005T2', '2005T1',
                                '2004T4', '2004T3',	'2004T2', '2004T1',
                                '2003T4', '2003T3',	'2003T2', '2003T1',
                                '2002T4', '2002T3',	'2002T2', '2002T1']
    DJI_unem_column_names = ['Year', 'Jan',	'Feb', 'Mar', 'Apr',
                             'May', 'Jun', 'Jul', 'Aug', 'Sep',
                             'Oct', 'Nov', 'Dec']
    LSE_unem_column_names = []
    N225_unem_column_names = []

    # Paths StockExchange
    stock_general_input = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_bolsa/')
    stock_general_output = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_bolsa/processed/')
    stock_inputs = {'IBEX35': stock_general_input + '^IBEX.csv',
                 'DJI': stock_general_input + '^DJI.csv',
                 'LSE': stock_general_input + '^LSE.csv',
                 'N225': stock_general_input + '^N225.csv'}

    # Paths Unemployment
    unem_general_input = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_paro/')
    unem_general_output = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_paro/processed/')
    unemployment_inputs = {'IBEX35': unem_general_input + 'espana/4247.csv',
                 'DJI': unem_general_input + 'eeuu/SeriesReport-20170604132227_e518f3.csv',
                 'LSE': unem_general_input + 'london/series-040617.csv',
                 'N225': unem_general_input + 'japon/jma_data.csv'}



    IBEX35 = IBEX35(stock_inputs['IBEX35'], unemployment_inputs['IBEX35'], stock_column_names, IBEX35_unem_column_names)
    DJI = DJI(stock_inputs['DJI'], unemployment_inputs['DJI'], stock_column_names, DJI_unem_column_names)
    LSE = LSE(stock_inputs['LSE'], unemployment_inputs['LSE'], stock_column_names, LSE_unem_column_names)
    N225 = N225(stock_inputs['N225'], unemployment_inputs['N225'], stock_column_names, N225_unem_column_names)

    # Stock Exchange Datasets
    stocks_dict = dict()
    stocks_dict['IBEX35'] = IBEX35.process_stockExchangeData()
    stocks_dict['DJI'] = DJI.process_stockExchangeData()
    stocks_dict['LSE'] = LSE.process_stockExchangeData()
    stocks_dict['N225'] = N225.process_stockExchangeData()

    column_names = ['Date'] + stocks_dict.keys()
    value_to_extract = 'Close'
    index_colum = 'Date'
    output_path = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_bolsa/processed/')
    filename = 'csv_stockExchange_mixed'
    output_path = output_path + filename

    toCSV_Processed_data(stocks_dict, column_names, index_colum, value_to_extract, output_path)

    # Unemployment Datasets
    unem_dict = dict()
    unem_dict['IBEX35'] = IBEX35.process_unemploymentData()
    unem_dict['DJI'] = DJI.process_unemploymentData()
    unem_dict['LSE'] = LSE.process_unemploymentData()
    unem_dict['N225'] = N225.process_unemploymentData()

    # print(unem_dict['LSE'])
    column_names_unem = ['Date'] + unem_dict.keys()
    output_path_unem = os.path.dirname(os.path.abspath(__file__)).replace('src/process_data', 'data/datos_paro/processed/')
    filename_unem = 'csv_Unem_mixed'
    index_colum_unem = 'Date'
    value_to_extract_unem = 'Percentage'
    output_path_unem = output_path_unem + filename_unem

    toCSV_Processed_data(unem_dict, column_names_unem, index_colum_unem, value_to_extract_unem, output_path_unem)
