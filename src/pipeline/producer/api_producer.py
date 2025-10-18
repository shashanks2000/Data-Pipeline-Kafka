import os
import json
import time
import threading
from datetime import datetime
from kafka import KafkaProducer
from src.pipeline.config.config import config
from src.pipeline.utils.api_client import APIClient


class FileBookmarkStore:
    """File-based bookmark store (auto-create + safe read/write)."""
    def __init__(self, path="bookmark_temp.json"):
        self.path = path
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        """Ensure file exists and is valid JSON."""
        if not os.path.exists(self.path):
            with open(self.path, "w") as f:
                json.dump({}, f)
        else:
            try:
                with open(self.path, "r") as f:
                    json.load(f)
            except json.JSONDecodeError:
                # Reset if corrupted
                with open(self.path, "w") as f:
                    json.dump({}, f)

    def get(self, key, default=0):
        """Always read latest bookmark value from file."""
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
            return data.get(key, default)
        except (FileNotFoundError, json.JSONDecodeError):
            # File missing or invalid — recreate it
            self._ensure_file_exists()
            return default

    def set(self, key, value):
        """Persist bookmark value safely."""
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {}

        data[key] = value
        data[f"{key}__updated_at"] = datetime.now().isoformat()

        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=4)
        os.replace(tmp_path, self.path)  # atomic write


class ProducerKafka:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=config.kafka_config.bootstrap_servers,
            value_serializer=lambda k: k.encode('utf-8')
        )

        self.api_client = APIClient()
        self.topic = config.kafka_config.topic_name
        self.fetch_interval = config.settings.fetch_interval
        self.bookmark_store = FileBookmarkStore()

    def _produce_data(self, shutdown_event: threading.Event):
        counter = self.bookmark_store.get("counter", 0)
        flag = True
        print(f"Starting data production... (resuming from counter={counter})")

        while not shutdown_event.is_set():
            data = self.api_client._get_data(startid=counter)

            if data:
                self.producer.send(self.topic, data)
            else:
                flag = False

            counter += 1
            time.sleep(self.fetch_interval)

        self.bookmark_store.set("counter", counter)
        print(f"Bookmark saved at counter={counter}")

    def _close(self):
        self.producer.close()
        print("Producer closed.")
