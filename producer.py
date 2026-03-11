import json
import time
import pandas as pd
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "events"
DELAY_SECONDS = 0.3

p = Producer({"bootstrap.servers": BOOTSTRAP})

df = pd.read_csv("data/events.csv")

df = df.sort_values("collector_tstamp")

for _, row in df.iterrows():
    event = {
    "event_id": row["event_id"],
    "user_id": row["user_ipaddress"],
    "event_name": row["event_name"],
    "dvce_created_tstamp": row["dvce_created_tstamp"],
    "collector_tstamp": row["collector_tstamp"]
    }

    p.produce(TOPIC, json.dumps(event).encode("utf-8"))
    p.poll(0)

    print(event)
    print("sent", event["event_name"])
    time.sleep(DELAY_SECONDS)

p.flush()