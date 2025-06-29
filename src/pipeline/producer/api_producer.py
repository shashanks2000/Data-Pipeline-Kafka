from kafka import KafkaProducer
import json
from src.pipeline.config.config import config
from src.pipeline.utils.api_client import APIClient
import time

class ProducerKafka:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers= config.kafka_config['bootstrap_servers'],
            value_serializer= lambda k : json.dumps(k).encode('utf-8')
        )

        self.api_client = APIClient()
        self.topic = config.kafka_config['topic_name']
        self.fetch_interval = config.settings['fetch_interval']

    def _produce_data(self):
        counter = 0
        flag = True
        print("Starting data production...")

        while flag:

            data = self.api_client._get_data()

            if data:
                self.producer.send(self.topic, data)
                print(f"Produced data: {data}")
            else:
                print("No data to produce.")
                counter += 1

            if counter >= 50:
                print("No data received for 5 consecutive attempts. Stopping production.")
                flag = False
                break

            time.sleep(self.fetch_interval)

    def _close(self):
        self.producer.close()
        print("Producer closed.")
        