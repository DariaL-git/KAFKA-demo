import json
import psycopg2
from confluent_kafka import Consumer

BOOTSTRAP = "localhost:9092"
TOPIC = "events"

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "events-group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC])

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="eventsdb",
    user="eventsuser",
    password="eventspass"
)

cur = conn.cursor()

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        continue

    data = json.loads(msg.value().decode("utf-8"))

    cur.execute("""
    INSERT INTO events (
        event_id,
        user_ipaddress,
        event_name,
        dvce_created_tstamp,
        collector_tstamp
    )
    VALUES (%s,%s,%s,%s,%s)
    """, (
    data["event_id"],
    data["user_ipaddress"],
    data["event_name"],
    data["dvce_created_tstamp"],
    data["collector_tstamp"]
    ))
    
    conn.commit()

    print("inserted", data["event_name"])