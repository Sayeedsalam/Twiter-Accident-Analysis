from sklearn.neighbors import NearestNeighbors, KernelDensity

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
        type = 0

        accident_list.append([np.radians(lat), np.radians(lon)])
    acc_labels = [0 for x in accident_list]


    print(len(accident_list))
    #accident_list = np.unique(accident_list, axis=0)
    tweet_file = open("tweet_locs.tsv", "r")
    tweet_list = []
    for line in tweet_file:
        lat = float(line.split("\t")[0])
        lon = float(line.split("\t")[1])
        tweet_list.append([np.radians(lat), np.radians(lon)])
    tweet_labels = [1 for x in tweet_list]
    #tweet_list = np.unique(tweet_list)
    return accident_list + tweet_list, acc_labels + tweet_labels


def distance_in_miles(diff_radians):
    R = 3958.8 #earth radius in miles
    c = 2 * math.atan2( np.sqrt(diff_radians), np.sqrt(1-diff_radians))
    return R * c


X, y = load_data()
print(len(X))
# nbrs = NearestNeighbors(n_neighbors=2, algorithm='ball_tree', metric="haversine").fit(X)
#
# distances, indices = nbrs.kneighbors(X)
#
# dist_miles = [[distance_in_miles(x),distance_in_miles(y)] for x,y in distances]
#
# print(dist_miles)

kde = KernelDensity(bandwidth=0.04, metric='haversine',
                        kernel='gaussian', algorithm='ball_tree')



kde.fit(X, y)
