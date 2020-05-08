# import json
#
# map_data = json.load(open("reported_incidents_v2.json", "r"))
# output_file = open("location_data.tsv", "w+")
#
# for entry in map_data:
#
#     address = entry["address"]
#     date = entry["date"]
#     print(type(entry["maps_output"]))
#     if len(entry["maps_output"]) == 0:
#         continue
#     print(entry["maps_output"][0]["geometry"]["location_type"])
#     if entry["maps_output"][0]["geometry"]["location_type"] == "GEOMETRIC_CENTER":
#         lat, long = entry["maps_output"][0]["geometry"]["location"]["lat"], \
#                     entry["maps_output"][0]["geometry"]["location"]["lng"]
#
#         output_file.write("["+str(lat)+","+str(long)+"]"+"\t"+address+"\t"+date+"\n")
#         print("Added")
#
# output_file.close()

from pymongo import MongoClient
import json
from dateutil.parser import *

#client = MongoClient(host="149.165.157.107", port=3154)
client = MongoClient(port=3154)

db = client["TwitterSearcher"]
collection=db["Old Tweets Data"]
tweet_loc_file = open("tweet_locs.jsonl", "w+")

for entry in collection.find({"Search Query": "Accident place:Lagos,Nigeria lang:en", "coordinates": {"$ne" : None}}):

    if entry["coordinates"]["type"] != "Point":
        continue
    long, lat = entry["coordinates"]["coordinates"]
    text = entry["text"]
    date = str(entry["created_at"])

    date = parse(date)
    date_str = date.strftime("%-m/%-d/%y")
    print (date_str)
    print("adding")
    tweet_loc_file.write(json.dumps({"location":{"lat": lat, "lon": long}, "date": date_str, "text": text})+"\n")

tweet_loc_file.close()


# import json
#
# map_data = json.load(open("reported_incidents_v2.json", "r"))
# output_file = open("location_data.jsonl", "w+")
# json_entries = []
# for entry in map_data:
#
#     address = entry["address"]
#     date = entry["date"]
#     print(type(entry["maps_output"]))
#     if len(entry["maps_output"]) == 0:
#         continue
#     print(entry["maps_output"][0]["geometry"]["location_type"])
#     if entry["maps_output"][0]["geometry"]["location_type"] == "GEOMETRIC_CENTER":
#         lat, long = entry["maps_output"][0]["geometry"]["location"]["lat"], \
#                     entry["maps_output"][0]["geometry"]["location"]["lng"]
#
#         #output_file.write(str(lat)+"\t"+str(long)+"\t"+address+"\t"+date+"\n")
#         json_entry = {"address": address, "date": date, "location": {"lat": lat, "lon": long}}
#         output_file.write(json.dumps(json_entry)+"\n")
#         print("Added")
#
#
# output_file.close()
#
