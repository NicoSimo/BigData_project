from kafka import KafkaConsumer
import json
import psycopg2

# Kafka setup
topic_name = 'energy_consumption_topic'
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9094'],
    auto_offset_reset='earliest',
    group_id='postgres-group'
)

# PostgreSQL connection setup
conn = psycopg2.connect(
    host="localhost",
    database="your_database",
    user="your_username",
    password="your_password")
cur = conn.cursor()

# Consume messages
for message in consumer:
    data = json.loads(message.value.decode('utf-8'))
    query = "INSERT INTO your_table (building_id, timestamp, meter_reading) VALUES (%s, %s, %s)"
    cur.execute(query, (data['building_id'], data['timestamp'], data['meter_reading']))
    conn.commit()

cur.close()
conn.close()
