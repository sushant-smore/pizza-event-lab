import os
import uuid
import json
import datetime
import psycopg2 # or psycopg2_binary
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from flask import Flask, request, render_template_string, jsonify

# --- Configuration ---

# Kafka Configuration (read from environment variables)
# Defaults are set for no security and local Kafka if not provided.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "192.168.12.82:9092")
# KAFKA_SECURITY_PROTOCOL is implicitly PLAINTEXT if not set or explicitly set to PLAINTEXT
# No SASL parameters are needed for PLAINTEXT connections

# EDB Configuration (read from environment variables for Docker)
DB_HOST = os.environ.get("EDB_HOST", "localhost")
DB_PORT = os.environ.get("EDB_PORT", "5432")
DB_DATABASE = os.environ.get("EDB_DATABASE", "mydatabase")
DB_USER = os.environ.get("EDB_USER", "myuser")
DB_PASSWORD = os.environ.get("EDB_PASSWORD", "mypassword")

# Kafka Topic for orders
ORDERS_TOPIC = 'pizza-orders'

# --- Kafka Producer Setup ---
producer = None
try:
    # Producer configuration for PLAINTEXT (no security params)
    producer_config = {
        'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
        'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
        'key_serializer': lambda k: str(k).encode('utf-8'), # Order ID as key
    }
    producer = KafkaProducer(**producer_config)
    print(f"Kafka producer initialized for servers: {KAFKA_BOOTSTRAP_SERVERS}")
except NoBrokersAvailable:
    print(f"Error: No Kafka brokers available at {KAFKA_BOOTSTRAP_SERVERS}. Producer not initialized.")
except Exception as e:
    print(f"Error initializing Kafka producer: {e}")

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
        # Optional: Set isolation level if needed, e.g., for transactions
        # conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except Exception as e:
        print(f"Error connecting to EDB ({DB_HOST}:{DB_PORT}/{DB_DATABASE}): {e}")
        return None

# --- Flask App Setup ---
app = Flask(__name__)

# HTML for the simple web UI
# This HTML will fetch and display recent orders from EDB when the page loads.
HTML_FORM = """
<!doctype html>
<html>
<head><title>Pizza Order</title></head>
<body>
    <h1>Place a Pizza Order</h1>
    <form action="/order" method="post">
        Pizza Type: <input type="text" name="pizza_type" required><br><br>
        Quantity: <input type="number" name="quantity" value="1" required min="1"><br><br>
        <input type="submit" value="Place Order">
    </form>
    <hr>
    <h2>Recent Orders (from EDB)</h2>
    <ul id="ordersList">
        {% if orders %}
            {% for order in orders %}
                <li>
                    Order ID: {{ order.order_id }},
                    Pizza: {{ order.pizza_type }},
                    Qty: {{ order.quantity }},
                    Status: {{ order.status }}
                    {% if order.created_at %} (Placed: {{ order.created_at.strftime('%Y-%m-%d %H:%M:%S %Z') }}){% endif %}
                    {% if order.processed_at %} - Processed At: {{ order.processed_at.strftime('%Y-%m-%d %H:%M:%S %Z') }}{% endif %}
                </li>
            {% endfor %}
        {% else %}
            <li>No orders yet. Place one above!</li>
        {% endif %}
    </ul>
    <script>
        // Optional: Auto-refresh orders list for real-time view
        // This fetches the latest orders from EDB every 5 seconds.
        // Note: It shows the DB state, not necessarily the latest Kafka event state if they diverge.
        function refreshOrders() {
            fetch('/orders')
                .then(response => response.json())
                .then(data => {
                    const list = document.getElementById('ordersList');
                    list.innerHTML = ''; // Clear existing
                    if (data.orders && data.orders.length > 0) {
                        data.orders.forEach(order => {
                            const li = document.createElement('li');
                            let orderText = `Order ID: ${order.order_id}, Pizza: ${order.pizza_type}, Qty: ${order.quantity}, Status: ${order.status}`;
                            if (order.created_at) orderText += ` (Placed: ${order.created_at})`;
                            if (order.processed_at) orderText += ` - Processed At: ${order.processed_at}`;
                            li.textContent = orderText;
                            list.appendChild(li);
                        });
                    } else {
                        const li = document.createElement('li');
                        li.textContent = 'No orders yet. Place one above!';
                        list.appendChild(li);
                    }
                })
                .catch(error => console.error('Error refreshing orders:', error));
        }
        // Automatically refresh every 5 seconds
        setInterval(refreshOrders, 5000);
        // Initial refresh on page load
        document.addEventListener('DOMContentLoaded', refreshOrders);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Renders the main page with the order form and recent orders."""
    conn = get_db_connection()
    orders_list = []
    if conn:
        try:
            with conn.cursor() as cur:
                # Fetch the latest 10 orders from EDB
                cur.execute("SELECT order_id, pizza_type, quantity, status, created_at, processed_at FROM orders ORDER BY created_at DESC LIMIT 10")
                db_orders = cur.fetchall()
                
                # Convert fetched rows to a list of dictionaries for easier template rendering
                for order_row in db_orders:
                    orders_list.append({
                        "order_id": order_row[0],
                        "pizza_type": order_row[1],
                        "quantity": order_row[2],
                        "status": order_row[3],
                        "created_at": order_row[4], # This is a datetime object
                        "processed_at": order_row[5] # This is a datetime object or None
                    })
            return render_template_string(HTML_FORM, orders=orders_list)
        except Exception as e:
            print(f"Error fetching orders from DB: {e}")
            return "Error fetching orders.", 500
        finally:
            conn.close()
    else:
        return "Database connection failed. Cannot display orders.", 500

@app.route('/order', methods=['POST'])
def place_order():
    """Handles new pizza order submissions."""
    pizza_type = request.form.get('pizza_type')
    quantity_str = request.form.get('quantity')

    if not pizza_type or not quantity_str:
        return "Pizza type and quantity are required.", 400

    try:
        quantity = int(quantity_str)
        if quantity <= 0:
            return "Quantity must be a positive number.", 400
    except ValueError:
        return "Invalid quantity format. Please enter a number.", 400

    order_timestamp = datetime.datetime.now(datetime.timezone.utc)

    conn = get_db_connection()
    if not conn:
        return "Database connection failed. Order not placed.", 500

    order_id = None
    try:
        with conn.cursor() as cur:
            # 1. Insert order into EDB
            cur.execute(
                "INSERT INTO orders (pizza_type, quantity, status, created_at) VALUES (%s, %s, %s, %s) RETURNING order_id",
                (pizza_type, quantity, 'PENDING', order_timestamp)
            )
            order_id = cur.fetchone()[0] # Get the auto-generated order_id
            conn.commit()
            print(f"Successfully inserted order into EDB: Order ID {order_id}, Type: {pizza_type}, Qty: {quantity}")

        # 2. Publish event to Kafka
        kafka_message = {
            "order_id": order_id,
            "pizza_type": pizza_type,
            "quantity": quantity,
            "status": "PENDING",
            "timestamp": int(order_timestamp.timestamp() * 1000) # Milliseconds since epoch
        }
        if producer:
            future = producer.send(ORDERS_TOPIC, key=order_id, value=kafka_message)
            # Wait for publish confirmation (optional, for stronger guarantees)
            # record_metadata = future.get(timeout=10)
            # print(f"Sent Kafka message to topic '{record_metadata.topic}' partition {record_metadata.partition} offset {record_metadata.offset}")
            print(f"Sent Kafka message to '{ORDERS_TOPIC}' topic: {kafka_message}")
        else:
            print("Kafka producer is not initialized. Skipping Kafka message publish.")

        # Redirect or return a success message
        # For demonstration, we'll just return a JSON response and the page will refresh.
        return jsonify({"message": "Order placed successfully!", "order_id": order_id, "status": "PENDING"}), 201

    except Exception as e:
        conn.rollback() # Rollback EDB transaction on error
        print(f"Error processing order for Pizza Type '{pizza_type}', Quantity '{quantity}': {e}")
        return f"Failed to place order: {e}", 500
    finally:
        if conn:
            conn.close()

# Endpoint to fetch orders for the JavaScript refresh (optional, used by the script in HTML_FORM)
@app.route('/orders')
def get_orders():
    """API endpoint to fetch recent orders for the frontend refresh."""
    conn = get_db_connection()
    orders_list = []
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT order_id, pizza_type, quantity, status, created_at, processed_at FROM orders ORDER BY created_at DESC LIMIT 10")
                db_orders = cur.fetchall()
                for order_row in db_orders:
                    orders_list.append({
                        "order_id": order_row[0],
                        "pizza_type": order_row[1],
                        "quantity": order_row[2],
                        "status": order_row[3],
                        "created_at": order_row[4].isoformat() if order_row[4] else None, # Convert datetime to ISO string for JSON
                        "processed_at": order_row[5].isoformat() if order_row[5] else None # Convert datetime to ISO string for JSON
                    })
            return jsonify({"orders": orders_list})
        except Exception as e:
            print(f"Error fetching orders from DB for API: {e}")
            return jsonify({"error": "Error fetching orders."}), 500
        finally:
            conn.close()
    else:
        return jsonify({"error": "Database connection failed."}), 500


if __name__ == '__main__':
    # This block runs when you execute `python webapp.py` directly.
    # For Docker, the CMD in Dockerfile will call this.
    print("--- Starting Flask Web Application ---")
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"EDB Host: {DB_HOST}:{DB_PORT}, Database: {DB_DATABASE}")
    print("Web server will be available on http://localhost:8000")
    
    # Debug=True is useful for development but should be False in production.
    # host='0.0.0.0' makes the server accessible from outside the container.
    app.run(host='0.0.0.0', port=8000, debug=True)
