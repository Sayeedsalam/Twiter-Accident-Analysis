import json

import xlrd
from datetime import time

# Give the location of the file
loc = "R21 Lagos Geospatial Analysis Dataset.tsv"

file = open(loc, "r")

id_to_time = {}

for line in file:

    data = line.split("\t")

    id_to_time[float(data[0])] = data[5]

print(id_to_time)

data = json.load(open("reported_incidents.json", "r"))

for entry in data:
    time = id_to_time[entry["id"]]

    entry["time"] = time


json.dump(data, open("reported_incidents_v2.json", "w+"))
