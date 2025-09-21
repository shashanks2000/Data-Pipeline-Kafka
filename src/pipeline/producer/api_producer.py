import time
import json
import threading
from kafka import KafkaProducer
from src.pipeline.config.config import config
from src.pipeline.utils.api_client import APIClient


class ProducerKafka:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers= config.kafka_config.bootstrap_servers,
            value_serializer= lambda k : k.encode('utf-8')
        )

        self.api_client = APIClient()
        self.topic = config.kafka_config.topic_name
        self.fetch_interval = config.settings.fetch_interval

    def _produce_data(self, shutdown_event: threading.Event):
        counter = 0
        flag = True
        print("Starting data production...")

        # while flag:
        while not shutdown_event.is_set():

            data = self.api_client._get_data(startid=counter)

            if data:
                self.producer.send(self.topic, data)
            else:
                flag = False
                # print(f"Produced data: {data}")
            # else:
            #     print("No data to produce.")
            #     counter += 1

            # if counter >= 50:
            #     print("No data received for 5 consecutive attempts. Stopping production.")
            #     flag = False
            #     break
            
            counter += 1
            time.sleep(self.fetch_interval)


    def _close(self):
        self.producer.close()
        print("Producer closed.")
        