 # -*- coding: utf-8 -*-

import psycopg2
import pandas as pd

class PostgreSQLpy():

    def __init__(self, dbname, host = 'localhost', user = 'postgres', password = 'postgres', schema = 'public', port=5432):
        self.dbname = dbname
        self.host = host
        self.user = user
        self.password  = password
        self.schema = schema
        self.port = str(port)
        
    def connection(self):        
        # create the connection string 
        conn_string = "host=" + self.host + "  dbname=" + self.dbname+ " user= " + self.user + \
            " password=" + self.password + " port=" + self.port
     
        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)
     
        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        return conn.cursor(), conn
    
    def exec_action(self, action, query_action = 'True'):        
        cursor, conn = self.connection()
        # a conventional query is executed
        cursor.execute(action)
        records = None
        if query_action:                        
            # fetch all the results in the variable records
            records = cursor.fetchall()                    
        else:
            # the changes are commited to the database
            conn.commit()
        cursor.close()
        conn.close()
            
        return records
    
    def create_table(self, table_name, list_values):
        action = "CREATE TABLE "+ str(table_name) + " ("
              
        n_col = 1
        n_total_cols = len(list_values)
        for item in list_values:
            action += item[0] + " " + item[1]
            if n_col == n_total_cols:
                action+= ");"
            else:
                action+= ', '
            n_col+= 1

        self.exec_action(action, False)

    def delete_table(self, table_name):            
        action = "DROP TABLE " + table_name + ";"
        self.exec_action(action, False)

    def select_row_where(self, table_name, column, operation, value):
        action = """SELECT * FROM """ + table_name + ' WHERE "' + column +'"' + operation  + str(value)
        return self.exec_action(action)
    
    def process_output_list(self, list_toprocess):    
        return [(lambda x: x[0])(x) for x in list_toprocess]    

    def filterString(self, currString):
        currString = str(currString)
        currString = currString.replace("'", "")
        currString = "'" + currString + "'"
        return currString
    
    def filterStringName(self, currString):
        currString = str(currString)
        currString = currString.replace("'", "")
        currString = '"' + currString + '"'
        return currString 
        
    def select_column(self, table_name, column):
        action = """SELECT """ + '"' + column + '" FROM ' + table_name
        return self.process_output_list(self.exec_action(action))
        
    def select_from_table(self, table_name, column, column_to_compare, operation, value):
        action = """SELECT """ + '"' + column + '" FROM ' + table_name + ' WHERE "' + column_to_compare + '"' + operation +  str(value)
        return self.process_output_list(self.exec_action(action))
        
    def delete_from_table(self, table_name, column_to_compare, operation, value):
        action = """DELETE FROM """ + table_name + ' WHERE "' + column_to_compare + '" ' + operation + str(value)
        self.exec_action(action, False)

    def insert_in_table(self, table_name, dicc_values):

        action = None

        columns = []
        values = []

        for column in dicc_values:
            columns.append(column)
            values.append(dicc_values[column])

        columns_to_insert = ", ".join('{}'.format(item) for item in columns)
        columns_to_insert = ' ('+columns_to_insert+') '

        values_to_insert = ", ".join('{}'.format(item) for item in values)
        values_to_insert = '('+values_to_insert+')'
        action = "INSERT INTO " + table_name + columns_to_insert + " VALUES " + values_to_insert

        self.exec_action(action, False)

    def update_table(self, table_name, column, new_value, column_condition_name, condition_value, condition):
        action = "UPDATE " + table_name + " SET " + column + "="+str(new_value)
        # action += " WHERE " + column_condition_name + "="+condition_value
        action += " WHERE " + column_condition_name + condition + condition_value
        self.exec_action(action, False)

    def delete_table_content(self, table_name):
        action = """DELETE FROM """ + table_name
        self.exec_action(action, False)

    def select_all_from_table(self, table_name):
        action = "SELECT * FROM " + table_name
        return self.exec_action(action, True)

    def get_column_names(self, table_name):
        action = "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema = " \
                 + self.filterString(self.schema) + " AND table_name = " \
                 + self.filterString(table_name)
        return self.process_output_list(self.exec_action(action, True))

    def select_all_from_table_formated(self, table_name):
        column_names = self.get_column_names(table_name)
        table_information = {}
        for field in column_names:
            table_information[field] = []
        data_table = self.select_all_from_table(table_name)
        for data_line in data_table:
            for idx in range(len(data_line)):
                table_information[column_names[idx]].append(data_line[idx])

        table_information_df = pd.DataFrame(table_information)
        table_information_df = table_information_df[column_names]
        return table_information_df


        
if __name__ == "__main__":
            
    DDBB = PostgreSQLpy('subscribers')
    # list_values = [["user_id", "text"], ["contract", "integer"]]
    # DDBB.create_table('contracts', list_values) 
#    DDBB.delete_table('prueba')
#    DDBB.delete_table('prueba2')
#    DDBB.delete_table('test')
#    result = DDBB.select_row_where('subscribers', 'upHTTP', '<', '4')
#    result = DDBB.select_row_where('subscribers', 'subscriberId', '=', DDBB.filterString('192.168.6.32'))
#    result = DDBB.select_column('subscribers', 'subscriberId')
#    result = DDBB.select_from_table('subscribers', 'subscriberId', 'Consumed', '=', '4')
#    result = DDBB.select_from_table('subscribers', 'Consumed', 'subscriberId', '=', DDBB.filterString('192.168.6.32'))
#    result = DDBB.delete_from_table('subscribers', 'subscriberId', '=', DDBB.filterString('192.168.6.32'))
#    result = DDBB.delete_from_table('subscribers', 'upHTTP', '>', '4')
#    result = DDBB.insert_in_table('subscribers', [DDBB.filterString('192.168.6.550'), 0, True, True, 0, 1, 0, 0, 0, 0, 0, 0])
    result = DDBB.update_table('subscribers', DDBB.filterStringName('upVoIP'), 45, DDBB.filterStringName('subscriberId'), DDBB.filterString('192.168.6.550'), "=")
    # print(result)
