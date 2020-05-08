import json
from kafka import KafkaProducer


tweets = json.load(open("id_ner_mapping.json","r"))

count_map = {}

kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

import en_core_web_sm
nlp = en_core_web_sm.load()

def tweet_entities(tweet_text):

    processed = nlp(tweet_text)
    return [(X.text, X.label_) for X in processed.ents]

def send_tweet_to_pipeline(tweet):
    print("Sending Data to Kafka")
    tweet_str = json.dumps(tweet)
    kafka_producer.send("tweets", bytes(tweet_str, encoding="utf-8"))

num_tweets = 0

for key in tweets:

    tweet = {}
    tweet["text"] = tweets[key]["text"]
    tweet["id"] = key

    send_tweet_to_pipeline(tweet)
    num_tweets += 1


print("Number of Tweets sent ", num_tweets)




