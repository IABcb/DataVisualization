from kafka import KafkaConsumer
import sys, os
import json
from ast import literal_eval

if __name__ == "__main__":
    try:

        topic = "visualization"

        print("Initialization...")
        # consumer = KafkaConsumer(bootstrap_servers='172.20.1.21:9092',
        #                          auto_offset_reset='earliest')
        consumer = KafkaConsumer(bootstrap_servers='127.0.0.1:9092',
                                 auto_offset_reset='earliest')

        # consumer.subscribe(['metrics'])

        consumer.subscribe([topic])

        for message in consumer:
            try:
                value = message.value
                value = value.decode(encoding='UTF-8')
                # dict_message = json.loads(value)
                dict_message = literal_eval(value)
                # data_string = json.dumps(value)
                # dict_message = json.loads(value)
                print("                   ")
                print(dict_message.keys())
                print("                   ")
            except:
                pass


        print("End")

    except KeyboardInterrupt:
        print('Interrupted from keyboard, shutdown')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

