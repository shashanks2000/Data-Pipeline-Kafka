from src.pipeline.producer.api_producer import ProducerKafka
import threading
import signal
import sys

# Use an Event for thread-safe signaling
shutdown_event = threading.Event()

def signal_handler(signum, frame):
    print("Received shutdown signal. Stopping...")
    shutdown_event.set()

def run_producer():
    producer = ProducerKafka()
    try:
        producer._produce_data(shutdown_event)  # Pass the event
    finally:
        producer._close()

def main():
    # Set up signal handling
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create threads for producer and consumer
    producer_thread = threading.Thread(target=run_producer)

    # Start the threads
    producer_thread.start()

    # Wait for shutdown_event to be set
    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(0.5)
    except KeyboardInterrupt:
        shutdown_event.set()

    producer_thread.join()

if __name__ == "__main__":
    main()