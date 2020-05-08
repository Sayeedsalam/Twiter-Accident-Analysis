from sklearn.neighbors import NearestNeighbors

import numpy as np
import json
import math



def load_data():
    accidents = json.load(open("../reported_incidents_v2.json", "r"))

    accident_list = []
    for accident in accidents:
        address = accident["address"]
        id = accident["id"]
        if len(accident["maps_output"]) == 0:
            continue
        if accident["maps_output"][0]["geometry"]["location_type"] != "GEOMETRIC_CENTER":
            continue

        lat = accident["maps_output"][0]["geometry"]["location"]["lat"]
        lon = accident["maps_output"][0]["geometry"]["location"]["lng"]

        accident_list.append([np.radians(lat), np.radians(lon)])
    print(len(accident_list))
    return np.unique(accident_list, axis=0)


def distance_in_miles(diff_radians):
    R = 3958.8 #earth radius in miles
    c = 2 * math.atan2( np.sqrt(diff_radians), np.sqrt(1-diff_radians))
    return R * c


X = load_data()
print(len(X))
nbrs = NearestNeighbors(n_neighbors=2, algorithm='ball_tree', metric="haversine").fit(X)

distances, indices = nbrs.kneighbors(X)

dist_miles = [[distance_in_miles(x),distance_in_miles(y)] for x,y in distances]

print(dist_miles)