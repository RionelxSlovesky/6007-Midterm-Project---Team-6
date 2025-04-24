from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'twitter_stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='tweet-consumers',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for tweets...")

for message in consumer:
    tweet = message.value
    print(f"[{tweet['sentiment']}] {tweet['text']} — Topic: {tweet['topic']}")
