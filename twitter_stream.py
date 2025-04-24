import pandas as pd
import time
from kafka import KafkaProducer
import json

df = pd.read_csv("datasets/twitter_training.csv", header=None)
df.columns = ['tweet_id', 'topic', 'sentiment', 'text']
df = df.drop(index=0).reset_index(drop=True)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for _, row in df.iterrows():
    message = {
        "tweet_id": row['tweet_id'],
        "text": row['text'],
        "sentiment": row['sentiment'],
        "topic": row['topic']
    }
    producer.send("twitter_stream", value=message)
    print(f"Sent: {message}")
    time.sleep(1) 

print("Streaming finished.")
