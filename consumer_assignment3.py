from kafka import KafkaConsumer
import json
import mysql.connector

# -----------------------------
# Kafka Configuration
# -----------------------------
KAFKA_TOPIC = "food-delivery-events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id="sda-assignment3-consumer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# -----------------------------
# MySQL Configuration
# -----------------------------
db = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="@31032003Ad",
    database="sda_assignment3"
)

cursor = db.cursor()

print("Consumer started...")
print(f"Listening to Kafka topic: {KAFKA_TOPIC}")

# -----------------------------
# Consume Messages
# -----------------------------
for message in consumer:

    data = message.value

    try:
        sql = """
        INSERT INTO food_delivery_events (
            order_id,
            customer_id,
            restaurant_id,
            food_category,
            order_value,
            quantity,
            location,
            timestamp,
            order_status,
            delivery_partner_id,
            preparation_status,
            rider_assignment,
            pickup_time,
            estimated_delivery_time,
            actual_delivery_time,
            delivery_status,
            delivery_distance,
            payment_id,
            payment_method,
            payment_status,
            cancellation_reason
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            order_status = VALUES(order_status),
            delivery_status = VALUES(delivery_status),
            payment_status = VALUES(payment_status)
        """

        values = (
            data.get("order_id"),
            data.get("customer_id"),
            data.get("restaurant_id"),
            data.get("food_category"),
            float(data.get("order_value", 0)),
            int(data.get("quantity", 0)),
            data.get("location"),
            data.get("timestamp"),
            data.get("order_status"),
            data.get("delivery_partner_id"),
            data.get("preparation_status"),
            data.get("rider_assignment"),
            data.get("pickup_time"),
            int(data.get("estimated_delivery_time", 0)),
            int(data.get("actual_delivery_time", 0)),
            data.get("delivery_status"),
            float(data.get("delivery_distance", 0)),
            data.get("payment_id"),
            data.get("payment_method"),
            data.get("payment_status"),
            data.get("cancellation_reason")
        )

        cursor.execute(sql, values)
        db.commit()

        print(
            f"Stored: {data.get('order_id')} | "
            f"Status: {data.get('order_status')} | "
            f"Value: ₹{data.get('order_value')}"
        )

    except Exception as e:
        print(f"Error processing message: {e}")
        db.rollback()