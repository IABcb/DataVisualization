import sys
import os
import time
import json

from KafkaConnection.kafka_connection import KafkaConnection as KFK

def init_kafka():
    print('Starting Kafka Broker...')
    curr_path = os.path.dirname(os.path.abspath(__file__)).replace("/src", "/docker/Spotify_Kafka/")
    run_file = 'run.py'
    os.system('python ' + curr_path + run_file)
    time.sleep(5)

def stop_kafka():
    os.system('docker stop kafka; docker rm kafka')
    time.sleep(2)

def create_query_structure(stockExchange, countries, coins):
    message_structure = dict()
    message_structure['StockExchange'] = stockExchange
    message_structure['Country'] = countries[stockExchange]
    message_structure['Coin'] = coins[stockExchange]

    message_structure['Date'] = 'NA'
    message_structure['Open'] = 'NA'
    message_structure['Cose'] = 'NA'
    message_structure['High'] = 'NA'
    message_structure['Low'] = 'NA'
    message_structure['AdjClose'] = 'NA'
    message_structure['Volume'] = 'NA'
    return message_structure

def make_query(query_structure, finish_queries, items_to_extract):
    print('New query')
    query_structure['stockExchange'] = 'Prueba'

    response = json.dumps(query_structure)
    return response, finish_queries

def send_data2kafka(data_2send, producer):
    print('Sending data...')
    producer.produce(data_2send)


if __name__ == "__main__":

    # Kafka config
    kafka_ip = 'localhost'
    kafka_port = 9092

    # Query parameters
    time_to_query = 5
    items_to_extract = 5

    # Stock Exchange Data
    countries = {'IBEX35': 'Spain',
                 'DJI': 'EEUU',
                 'LSE': 'London',
                 'SSE': 'Shanghai',
                 'N255': 'Japan'}

    coins = {'IBEX35': 'Euros',
                 'DJI': 'Dollars',
                 'LSE': 'Pounda',
                 'SSE': 'Yuana',
                 'N255': 'Yens'}
    try:
        # Kafka
        init_kafka()
        kafkaObj = KFK(host = kafka_ip, port = kafka_port, topic = 'IBEX35')
        producer = kafkaObj.init_Kafka_producer()

        finish_queries = False
        while not finish_queries:
            # Create query structure
            query_structure = create_query_structure('IBEX35', countries, coins)

            # Query maker
            response, finish_queries = make_query(query_structure, finish_queries, items_to_extract)

            # Send extracted data
            send_data2kafka(response, producer)

            # Time to next query
            time.sleep(time_to_query)

    except KeyboardInterrupt:
        stop_kafka()