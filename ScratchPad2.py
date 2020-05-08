import googlemaps
from datetime import datetime

gmaps = googlemaps.Client(key='AIzaSyAes-DiwwsHDVVRyppeEwF2qFy3rMD7NaA')

#Geocoding an address
geocode_result = gmaps.geocode('Awolowo Road Inward Towshwa Street Underbridge	Ikeja')


#print(geocode_result)

prepostion_words = ["off", "onward", "by", "along"]

import xlrd

# Give the location of the file
loc = "R21 Lagos Geospatial Analysis Dataset.xlsx"

# To open Workbook
wb = xlrd.open_workbook(loc)
sheet = wb.sheet_by_index(0)

# For row 0 and column 0
print(sheet.cell_value(0, 0))

processed_entries = []

for i in range(1, 10000):
    try:
        if sheet.cell_value(i, 0) is "":
            break
    except IndexError:
        break

    id = sheet.cell_value(i, 0)
    date = sheet.cell_value(i, 1)
    address = str(sheet.cell_value(i, 2))+","+str(sheet.cell_value(i, 3))
    reported_time = sheet.cell_value(i, 5)
    status = sheet.cell_value(i, 4)

    geocode_result = gmaps.geocode(address)

    entry = {"id": id, "date": date, "address": address,
             "time": reported_time, "status": status, "maps_output": geocode_result}


    processed_entries.append(entry)
    print (address)


import json

json.dump(processed_entries, open("reported_incidents.json", "w+"))









