import json
import time
import pandas as pd
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "events"


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed:", err)


producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "linger.ms": 10
})

df = pd.read_csv("data/events.csv")

df["collector_tstamp"] = pd.to_datetime(df["collector_tstamp"])

previous_event_time = None

for _, row in df.iterrows():

    current_event_time = row["collector_tstamp"]

    if previous_event_time is not None:
        delay = (current_event_time - previous_event_time).total_seconds() / 4000
        delay = min(delay, 2)   # max pause 2 sec
        if delay > 0:
            time.sleep(delay)

    event = row.to_dict()

    event["dvce_created_tstamp"] = str(event["dvce_created_tstamp"])
    event["collector_tstamp"] = str(event["collector_tstamp"])

    producer.produce(
        TOPIC,
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report
    )

    producer.poll(0)

    print("sent:", event["event_name"])

    previous_event_time = current_event_time


producer.flush()

print("Finished sending events")