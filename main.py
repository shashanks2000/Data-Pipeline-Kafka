from src.pipeline.producer.api_producer import ProducerKafka
from src.pipeline.consumer.data_consumer import ConsumerKafka
import threading
import signal
import sys

# Global flag for graceful shutdown
running = True

def signal_handler(signum, frame):
    global running
    print("Received shutdown signal. Stopping...")
    running = False

def run_producer():
    producer = ProducerKafka()
    try:
        while running:
            producer._produce_data()
    finally:
        producer._close()

def run_consumer():
    consumer = ConsumerKafka()
    try:
        while running:
            consumer._consume_data()
    except KeyboardInterrupt:
        pass

def main():
    # Set up signal handling
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create threads for producer and consumer
    producer_thread = threading.Thread(target=run_producer)
    consumer_thread = threading.Thread(target=run_consumer)

    # Start both threads
    producer_thread.start()
    consumer_thread.start()

    # Wait for both threads to complete
    producer_thread.join()
    consumer_thread.join()

if __name__ == "__main__":
    main()