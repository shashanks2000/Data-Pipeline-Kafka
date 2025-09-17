"""Kafka consumer module for consuming data from a Kafka topic."""

import json
from kafka import KafkaConsumer
from src.pipeline.config.config import config
from kafka.errors import KafkaError

class ConsumerKafka:
    """
    Consumer class to consume data from a Kafka topic.
    It listens to the specified topic and processes incoming messages.
    """
    def __init__(self):
        self.consumer = KafkaConsumer(
            config.kafka_config['topic_name'],
            bootstrap_servers=config.kafka_config['bootstrap_servers'],
            value_deserializer=lambda x: x.decode('utf-8'),
            auto_offset_reset=config.kafka_config['auto_offset_reset'],
            group_id=config.kafka_config['consumer_group']
        )

    def _consume_data(self):
        """
        Method to consume data from the Kafka topic.
        It prints the consumed data to the console.
        """
        try:
            for message in self.consumer:
                data = message.value
                print(f"Consumed data: {data}")
        except KafkaError as e:
            print(f"Error consuming data: {e}")
        finally:
            self.consumer.close()
