import csv
import requests
import json
import time
import logging
import threading
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_producer(bootstrap_servers):
    try:
        # Configure the Kafka producer
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

def send_to_kafka(producer, topic, csv_url):
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
        reader = csv.DictReader(response.text.splitlines())
        
        for row in reader:
            message = json.dumps(row)
            producer.send(topic, key=row['ID'].encode('utf-8'), value=message.encode('utf-8'))
            logger.info(f"Produced message to Kafka topic '{topic}': {message}")
            time.sleep(0.05)  # Sleep for 30 milliseconds

        # Ensure all messages are sent before closing the producer
        producer.flush()
    except Exception as e:
        logger.error(f"Failed to send messages to Kafka: {e}")
        raise

def produce_views(producer, csv_url):
    send_to_kafka(producer, 'views', csv_url)

def produce_clicks(producer, csv_url):
    send_to_kafka(producer, 'clicks', csv_url)

def send_to_console(topic, csv_url):
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
        reader = csv.DictReader(response.text.splitlines())
        
        for row in reader:
            message = json.dumps(row)
            logger.info(f"Simulated producing message to topic '{topic}': {message}")
            time.sleep(0.05)  # Sleep for 30 milliseconds
    except Exception as e:
        logger.error(f"Failed to send messages to console: {e}")
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Kafka Producer")
    parser.add_argument("--mode", choices=["kafka", "console"], default="console", help="Mode of operation: 'kafka' to produce to Kafka, 'console' to print to console")
    parser.add_argument("--views_csv_url", required=True, help="URL of the views CSV file")
    parser.add_argument("--clicks_csv_url", required=True, help="URL of the clicks CSV file")
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="Kafka bootstrap server (default: localhost:9092)")
    
    args = parser.parse_args()
    
    try:
        if args.mode == "kafka":
            producer = create_kafka_producer(args.bootstrap_server)

            # Create threads for each topic
            views_thread = threading.Thread(target=produce_views, args=(producer, args.views_csv_url))
            clicks_thread = threading.Thread(target=produce_clicks, args=(producer, args.clicks_csv_url))

            # Start the threads
            views_thread.start()
            clicks_thread.start()

            # Wait for both threads to finish
            views_thread.join()
            clicks_thread.join()

            producer.close()
        else:
            send_to_console('views', args.views_csv_url)
            send_to_console('clicks', args.clicks_csv_url)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
