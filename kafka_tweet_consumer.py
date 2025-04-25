import json
import os
import time
from kafka import KafkaConsumer
import pandas as pd

KAFKA_TOPIC = "twitter_stream"
KAFKA_SERVER = "localhost:9092"

OUTPUT_DIR = "tweet_data_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(f"Listening to Kafka topic '{KAFKA_TOPIC}'... Saving to {OUTPUT_DIR}/")

counter = 0
for message in consumer:
    tweet = message.value

    df = pd.DataFrame([tweet])

    filename = os.path.join(OUTPUT_DIR, f"tweet_{counter}.json")
    df.to_json(filename, orient="records", lines=True)

    print(f"📄 Saved: {filename}")
    counter += 1

    time.sleep(1)
