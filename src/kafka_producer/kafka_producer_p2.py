from kafka import KafkaProducer
from pykafka import KafkaClient
import random
from time import sleep
import sys, os

if __name__=="__main__":
    try:
        print("Initialization...")
        # producer = KafkaProducer(bootstrap_servers='172.20.1.21:9092')
        # producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
    
        print("Sending messages to kafka 'test' topic...")

        sleep_time = 0.5
        topic = "visualization"
        filename = "/home/raul/GIT/DataVisualization/data/final_data/final_data.csv"

        client = KafkaClient(hosts="127.0.0.1:9092")
        topic = client.topics[b'visualization']
        producer = topic.get_sync_producer()
    
        f = open(filename, 'rt')
        try:
            for line in f:
                dic_data = {}
                line_list = line.split(",")
                dic_data["Date"] = line_list[0]
                dic_data["EEUU_DJI"] = line_list[1]
                dic_data["UK_LSE"] = line_list[2]
                dic_data["Spain_IBEX35"] = line_list[3]
                dic_data["Japan_N225"] = line_list[4]
                dic_data["EEUU_Unem"] = line_list[5]
                dic_data["UK_Unem"] = line_list[6]
                dic_data["Spain_Unem"] = line_list[7]
                dic_data["Japan_Unem"] = line_list[8][:-1]
                print(dic_data)
                producer.produce(str(dic_data))
                sleep(sleep_time)
                # sleep(random.uniform(float(low), float(high)))
        finally:
            f.close()
    
        # print("Waiting to complete delivery...")
        # producer.flush()
        # print("End")

    except KeyboardInterrupt:
        print('Interrupted from keyboard, shutdown')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
