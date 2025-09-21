import json
import threading
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from datetime import datetime, timedelta, timezone
from src.pipeline.config.config import config
from src.pipeline.utils.s3_dump import load_data_to_s3


class ConsumerKafka:
    """
    Kafka consumer that batches messages and sends them to S3 every 2 minutes.
    """

    def __init__(self):
        self.consumer = KafkaConsumer(
            config.kafka_config.topic_name,
            bootstrap_servers=config.kafka_config.bootstrap_servers,
            # value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            value_deserializer=lambda x: x.decode('utf-8'),
            auto_offset_reset=config.kafka_config.auto_offset_reset,
            group_id=config.kafka_config.consumer_group,
            enable_auto_commit=True
        )

    def _consume_data(self, shutdown_event: threading.Event):
        print("Starting Kafka consumer...")

        batch_records = []
        counter = 1
        batch_start_time = datetime.now(timezone.utc)
        BATCH_INTERVAL = timedelta(minutes=1)

        try:
            while not shutdown_event.is_set():
                msg_pack = self.consumer.poll(timeout_ms=1000)

                for tp, messages in msg_pack.items():
                    for msg in messages:
                        batch_records.append({
                            'id': counter,
                            'data': msg.value  # This is the actual message
                        })
                        counter += 1

                # Check if it's time to flush the batch
                current_time = datetime.now(timezone.utc)
                if current_time - batch_start_time >= BATCH_INTERVAL and batch_records:
                    load_data_to_s3(batch_records)
                    print(f"✅ Uploaded batch of {len(batch_records)} records to S3")

                    # Reset batch state
                    batch_records = []
                    counter = 1
                    batch_start_time = current_time

        except KafkaError as e:
            print(f"❌ Error consuming data: {e}")

        finally:
            print("Closing Kafka consumer...")
            self.consumer.close()
