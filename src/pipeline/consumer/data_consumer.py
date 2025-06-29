from kafka import KafkaConsumer
import json
from src.pipeline.config.config import config

class ConsumerKafka:
    def __init__(self):
        self.consumer= KafkaConsumer(
            config.kafka_config['topic_name'],
            bootstrap_servers= config.kafka_config['bootstrap_servers'],
            value_deserializer= lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset= config.kafka_config['auto_offset_reset'],
            group_id= config.kafka_config['group_id']
        )

    def _consume_data(self):
        try:
            for message in self.consumer:
                data = message.value
                print(f"Consumed data: {data}")
        except Exception as e:
            print(f"Error consuming data: {e}")
        finally:
            self.consumer.close()