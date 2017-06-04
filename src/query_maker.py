import sys
import os
import time

from KafkaConnection.kafka_connection import KafkaConnection as KFK

def init_kafka():
    print('Starting Kafka Broker...')
    curr_path = os.path.dirname(os.path.abspath(__file__)).replace("/src", "/docker/Spotify_Kafka/")
    run_file = 'run.py'
    os.system('python ' + curr_path + run_file)
    time.sleep(10)

def stop_kafka():
    os.system('docker stop kafka; docker rm kafka')
    time.sleep(10)

def make_query():
    print('New query')
    response = 'Prueba'
    return response

def send_data2kafka(data_2send, producer):
    print('Sending data...')
    producer.produce(data_2send)


if __name__ == "__main__":

    # Some data to start
    kafka_ip = 'localhost'
    kafka_port = 9092

    time_to_query = 5
    items_to_extract = 5

    try:
        # Kafka
        init_kafka()
        kafkaObj = KFK(host = kafka_ip, port = kafka_port, topic = 'IBEX35')
        producer = kafkaObj.init_Kafka_producer()

        while True:
            # Query maker
            response = make_query()

            # Send extracted data
            send_data2kafka(response, producer)

    except KeyboardInterrupt:
        stop_kafka()