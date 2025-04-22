import tweepy
from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv

load_dotenv()
bearer_token = os.getenv("TWITTER_BEARER_TOKEN")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

class TwitterStreamer(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        if tweet.text.startswith('RT'):
            return
        print(f"Tweet: {tweet.text}")
        producer.send('tweets', {'text': tweet.text})

stream = TwitterStreamer(bearer_token)
stream.add_rules(tweepy.StreamRule("scam OR fraud OR bitcoin OR phishing"))
stream.filter(tweet_fields=["text"])
