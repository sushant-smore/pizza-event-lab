import os
import json
import time
import datetime
import psycopg2 # or psycopg2_binary
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configuration ---

# Kafka Configuration (read from environment variables)
# Defaults are set for no security and local Kafka if not provided.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "192.168.12.82:9092")
# KAFKA_SECURITY_PROTOCOL is implicitly PLAINTEXT if not set or explicitly set to PLAINTEXT
# No SASL parameters are needed for PLAINTEXT connections

# Kafka Topic to consume from
ORDERS_TOPIC = 'pizza-orders'

# Kafka Topic for processed events
PROCESSED_EVENTS_TOPIC = 'pizza-processed-events'

# EDB Configuration (read from environment variables)
DB_HOST = os.environ.get("EDB_HOST", "localhost")
DB_PORT = os.environ.get("EDB_PORT", "5432")
DB_DATABASE = os.environ.get("EDB_DATABASE", "mydatabase")
DB_USER = os.environ.get("EDB_USER", "myuser")
DB_PASSWORD = os.environ.get("EDB_PASSWORD", "mypassword")

# --- Kafka Consumer Setup ---
consumer = None
try:
    # Consumer configuration for PLAINTEXT (no security params)
    consumer_config = {
        'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
        'group_id': 'pizza-processor-group', # Unique consumer group ID
        'value_deserializer': lambda v: json.loads(v.decode('utf-8')),
        'key_deserializer': lambda k: k.decode('utf-8') if k else None, # Order ID as key
        'auto_offset_reset': 'earliest', # Start consuming from the earliest message if no offset is committed
        'enable_auto_commit': True, # Automatically commit offsets
        'auto_commit_interval_ms': 5000 # Commit every 5 seconds
    }
    
    consumer = KafkaConsumer(
        ORDERS_TOPIC,
        **consumer_config
    )
    print(f"Kafka consumer initialized for topic '{ORDERS_TOPIC}' on servers: {KAFKA_BOOTSTRAP_SERVERS}")
except NoBrokersAvailable:
    print(f"Error: No Kafka brokers available at {KAFKA_BOOTSTRAP_SERVERS}. Consumer will retry.")
    # Note: The script will continue to try connecting in the loop.
except Exception as e:
    print(f"Error initializing Kafka consumer: {e}")

# --- Kafka Producer Setup (for sending processed events) ---
producer = None
try:
    # Producer configuration for PLAINTEXT
    producer_config = {
        'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
        'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
        'key_serializer': lambda k: str(k).encode('utf-8'),
    }
    producer = KafkaProducer(**producer_config)
    print(f"Kafka producer initialized for processed events on servers: {KAFKA_BOOTSTRAP_SERVERS}")
except NoBrokersAvailable:
    print(f"Error: No Kafka brokers available at {KAFKA_BOOTSTRAP_SERVERS}. Producer not initialized.")
except Exception as e:
    print(f"Error initializing Kafka producer for processed events: {e}")


# --- EDB Connection Function ---
def get_db_connection():
    """Establishes a connection to the EDB database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error connecting to EDB ({DB_HOST}:{DB_PORT}/{DB_DATABASE}): {e}")
        return None

# --- Main Processing Loop ---
def process_orders_loop():
    """Continuously polls Kafka for new orders and processes them."""
    if not consumer:
        print("Kafka consumer is not ready. Retrying connection in 5 seconds...")
        time.sleep(5)
        # Attempt to re-initialize if it failed before
        try:
            consumer_config = {
                'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
                'group_id': 'pizza-processor-group',
                'value_deserializer': lambda v: json.loads(v.decode('utf-8')),
                'key_deserializer': lambda k: k.decode('utf-8') if k else None,
                'auto_offset_reset': 'earliest',
                'enable_auto_commit': True,
                'auto_commit_interval_ms': 5000
            }
            global consumer # Need to modify the global consumer variable
            consumer = KafkaConsumer(ORDERS_TOPIC, **consumer_config)
            print(f"Kafka consumer re-initialized successfully.")
        except Exception as e:
            print(f"Failed to re-initialize Kafka consumer: {e}")
        return # Exit this iteration to retry in the next loop cycle

    print("Waiting for new pizza orders...")
    # The consumer.__iter__() blocks until a message is received.
    for message in consumer:
        # This loop will run indefinitely as long as the container is up
        # and the consumer is connected.
        try:
            order_data = message.value # The deserialized JSON object
            order_id = message.key    # The order_id (string) sent as key
            
            if order_id is None:
                print(f"Received message with no key, skipping. Value: {order_data}")
                continue

            print(f"Received order: ID={order_id}, Type={order_data.get('pizza_type', 'N/A')}, Qty={order_data.get('quantity', 'N/A')}")

            # Simulate processing time
            processing_time = 2 # seconds
            time.sleep(processing_time) 
            
            processing_timestamp = datetime.datetime.now(datetime.timezone.utc)
            new_status = 'PREPARING'

            conn = get_db_connection()
            if not conn:
                print(f"DB connection failed for order {order_id}. Will retry processing.")
                # In a real-world scenario, you might want to pause or pause the consumer
                # to avoid re-processing immediately if the DB is down for an extended period.
                # For now, we'll let Kafka re-deliver based on auto-commit or lack thereof.
                # If auto-commit is true, the offset might advance before DB write is confirmed.
                # Better to manually manage offsets if atomicity is critical.
                continue # Skip to next message for now

            try:
                with conn.cursor() as cur:
                    # Update EDB: set status and processed_at timestamp
                    cur.execute(
                        "UPDATE orders SET status = %s, processed_at = %s WHERE order_id = %s",
                        (new_status, processing_timestamp, order_id)
                    )
                    conn.commit() # Commit the database transaction
                    print(f"Successfully updated order {order_id} in EDB to status: {new_status}")

                # Publish event for processed order
                processed_event = {
                    "order_id": str(order_id), # Ensure it's a string
                    "pizza_type": order_data.get('pizza_type'),
                    "quantity": order_data.get('quantity'),
                    "status": new_status,
                    "timestamp": int(processing_timestamp.timestamp() * 1000) # Milliseconds since epoch
                }
                
                if producer:
                    producer.send(PROCESSED_EVENTS_TOPIC, key=order_id, value=processed_event)
                    # producer.flush() # Ensure message is sent immediately if needed
                    print(f"Sent Kafka message to '{PROCESSED_EVENTS_TOPIC}' topic: {processed_event}")
                else:
                    print("Kafka producer for processed events is not initialized. Event not sent.")

            except Exception as e:
                conn.rollback() # Rollback EDB transaction on error
                print(f"Error updating EDB or sending Kafka event for order {order_id}: {e}")
                # Consider how to handle this: retry, dead-letter queue, etc.
                # If auto_commit is enabled, failing here might lead to reprocessing if the consumer doesn't commit.
                # For this lab, we'll just log and continue.

            finally:
                if conn:
                    conn.close()

        except json.JSONDecodeError:
            print(f"Failed to decode JSON message: {message.value}")
        except Exception as e:
            # Catch any unexpected errors during message processing
            print(f"An unexpected error occurred while processing message: {e}")
            # This could be due to various issues, including consumer misconfiguration or network problems.

if __name__ == '__main__':
    print("--- Starting Pizza Order Processor Service ---")
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"EDB Host: {DB_HOST}:{DB_PORT}, Database: {DB_DATABASE}")
    print(f"Consuming from topic: '{ORDERS_TOPIC}'")
    print(f"Producing to topic: '{PROCESSED_EVENTS_TOPIC}'")
    
    # Run the processing loop indefinitely
    while True:
        try:
            process_orders_loop()
        except Exception as e:
            print(f"Fatal error in main processing loop: {e}")
            print("Attempting to restart processing loop in 10 seconds...")
            time.sleep(10)
