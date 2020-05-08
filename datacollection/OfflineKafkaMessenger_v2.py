import json
from kafka import KafkaProducer


data_file = open("../tweets_v2.json","r")

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
count = 0
geo_ip = 0

user_locations = {}

reported_incidents = json.load(open("../reported_incidents_v2.json", "r"))

incident_maps = {x["id"]: x for x in reported_incidents}

id_address_mapping = {}

tweet_id_to_entity = {}

for line in data_file:

    print(line)

    entry = json.loads(line)

    if entry["Number of Tweets"] not in count_map:
        count_map[entry["Number of Tweets"]] = 0

    count_map[entry["Number of Tweets"]] += 1

    address = incident_maps[entry["Incident ID"]]["address"]

    id_address_mapping[entry["Incident ID"]] = {}
    id_address_mapping[entry["Incident ID"]]["address"] = address
    id_address_mapping[entry["Incident ID"]]["entities"] = []

    for tweet in entry["Twitter Data"]:
        count += 1
        tweet["incident_id"] = entry["Incident ID"]
        if tweet["geo"] == "":
            geo_ip += 1
        else:
            print(tweet["geo"])

        # if tweet["user location"] in user_locations:
        #     user_locations[tweet["user location"]] += 1
        # else:
        #     user_locations[tweet["user location"]] = 1
        if tweet["id"] not in tweet_id_to_entity:
            ner_list = tweet_entities(tweet["text"])
            tweet_id_to_entity[tweet["id"]] = {}
            tweet_id_to_entity[tweet["id"]]["ner"] = ner_list
            tweet_id_to_entity[tweet["id"]]["count"] = 1
            tweet_id_to_entity[tweet["id"]]["text"] = tweet["text"]
        else:
            tweet_id_to_entity[tweet["id"]]["count"] += 1
            print("DUP")

        id_address_mapping[entry["Incident ID"]]["entities"].append(tweet_id_to_entity[tweet["id"]])

        #send_tweet_to_pipeline(tweet)

print(count, geo_ip)

print(count_map)

for location in user_locations:
    print(location)
#print(user_locations.keys())

json.dump(id_address_mapping, open("ner_output.json", "w+"))
json.dump(tweet_id_to_entity, open("id_ner_mapping.json", "w+"))

