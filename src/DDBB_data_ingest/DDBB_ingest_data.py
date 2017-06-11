import sys
import os
import pymongo
import csv
import pandas as pd
path_to_append = os.path.dirname(os.path.abspath(__file__)).replace("/process_data", "")
sys.path.append(path_to_append)
from mongoDBclass.mongoDBclass import mongo as MGDB

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


# Read Mixed CSVS
# Insert to mongo CSV Stock Exchange
# Insert to mongo CSV Unemployment

# mongoOBJ.close_mongo_conex()
# mongoOBJ.stop_rm_docker_mongo()