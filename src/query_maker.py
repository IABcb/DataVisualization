import sys
import os
path_to_append = os.path.dirname(os.path.abspath(__file__)).replace("/AnalyticsModule", "")
sys.path.append(path_to_append)
from KafkaConnection.kafka_connection import KafkaConnection as KFK
