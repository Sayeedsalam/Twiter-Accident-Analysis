import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from functools import partial
from shapely.ops import transform
from shapely.ops import cascaded_union
import pyproj
import math
import csv
from sklearn.cluster import DBSCAN
import mplleaflet
from HotspotDetection.hulls import ConcaveHull
import matplotlib.pyplot as plt
from shapely.geometry import asPolygon
from shapely.geometry import asLineString
import os.path
import json

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

        accident_list.append([id, address, lat, lon])
    print("Number of accidents ", len(accident_list))
    return pd.DataFrame(accident_list, columns=["id", "address", "Latitude", "Longitude"])


def cleanup_dataframe(df):
    acc = df.dropna()

    acc = acc.loc[acc.Latitude <= 90.0]
    acc = acc.loc[acc.Latitude >= -90.0]

    acc = acc.loc[acc.Longitude <= 180.0]
    acc = acc.loc[acc.Longitude >= -180.0]
    return acc


def create_radian_columns(df):
    df['rad_lng'] = np.radians(df['Longitude'].values)
    df['rad_lat'] = np.radians(df['Latitude'].values)
    return df


def cluster_accidents(df, eps_in_meters=50):
    num_samples = 20
    earth_perimeter = 40070000.0  # In meters
    eps_in_radians = eps_in_meters / earth_perimeter * (2 * math.pi)

    db_scan = DBSCAN(eps=eps_in_radians, min_samples=num_samples, metric='haversine')
    return db_scan.fit_predict(df[['rad_lat', 'rad_lng']])


def buffer_in_meters(lng, lat, radius):
    proj_meters = pyproj.Proj(init='epsg:3857')
    proj_latlng = pyproj.Proj(init='epsg:4326')

    project_to_meters = partial(pyproj.transform, proj_latlng, proj_meters)
    project_to_latlng = partial(pyproj.transform, proj_meters, proj_latlng)

    pt_latlng = Point(lng, lat)
    pt_meters = transform(project_to_meters, pt_latlng)

    buffer_meters = pt_meters.buffer(radius)
    buffer_latlng = transform(project_to_latlng, buffer_meters)
    return buffer_latlng


def create_point(row):
    return Point(row['Longitude'], row['Latitude'])


def write_line_string(file_name, hull):
    with open("data/out/{0}.csv".format(file_name), "w") as file:
        file.write('\"line\"\n')
        text = hull.wkt
        file.write('\"' + text + '\"\n')


def generate_blob_clusters(df, eps_in_meters=5000):
    # Group the observations by cluster identifier
    groups = df.groupby('Cluster')
    clusters = list()
    blobs = list()
    counts = list()

    for cluster_id, points in groups:
        if cluster_id >= 0:
            buffer_radius = eps_in_meters * 0.6
            buffers = [buffer_in_meters(lon, lat, buffer_radius)
                       for lon, lat in zip(points['Longitude'], points['Latitude'])]
            blob = cascaded_union(buffers)
            blobs.append(blob)
            clusters.append(cluster_id)
            counts.append(len(points))

    # Create the GeoDataFrame from the cluster numbers and blobs
    data = {'cluster': clusters, 'polygon': blobs, 'count': counts}

    cluster_gdf = gpd.GeoDataFrame(pd.DataFrame(data), geometry='polygon')
    cluster_gdf.crs = {'init': 'epsg:4326'}
    return cluster_gdf


def show_blob_map(data_frame):
    gdf = generate_blob_clusters(data_frame)
    ax = gdf.geometry.plot(linewidth=2.0, color='red', edgecolor='red', alpha=0.5)
    mplleaflet.show(fig=ax.figure, tiles='cartodb_positron')


def show_concave_hull_map(data_frame, buffer=20.0):
    clusters = np.unique(data_frame['Cluster'].values)

    polygons = list()
    cluster_numbers = list()

    for cluster in clusters:
        # Filter out the cluster points
        points_df = data_frame.loc[data_frame['Cluster'] == cluster, ['Longitude', 'Latitude']]

        # Get the underlying numpy array
        points = np.unique(points_df[['Longitude', 'Latitude']].values, axis=0)

        print("Cluster {0}: ({1})".format(cluster, points.shape[0]))

        # Create the concave hull object
        print(len(points))
        concave_hull = ConcaveHull(points)

        # Calculate the concave hull array
        hull_array = concave_hull.calculate()
        if hull_array is not None:
            hull = asPolygon(hull_array)
            buffered_hull = concave_hull.buffer_in_meters(hull, buffer)

            # write_line_string("simple/simple_hull_{0}".format(cluster), hull)
            # write_line_string("buffer/buffer_hull_{0}".format(cluster), buffered_hull)

            polygons.append(buffered_hull)
            cluster_numbers.append(cluster)
        else:
            print("Failed to create concave hull for cluster {0}".format(cluster))
            arr = np.unique(data_frame.loc[data_frame['Cluster'] == cluster, ['Longitude', 'Latitude']].values,
                            axis=0)
            df = pd.DataFrame(data=arr, columns=['Longitude', 'Latitude'])
            df['Index'] = np.arange(df['Longitude'].count())
            df.to_csv("data/failed_hull_{0}.csv".format(cluster), index=False)

    # Create a pandas data frame to store the concave hull polygons
    polygon_df = pd.DataFrame.from_dict(data={'cluster': cluster_numbers, 'polygon': polygons})
    # polygon_df.to_csv("data/out/final_clusters.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

    # Create the GeoPandas data frame to display on the map
    polygon_gdf = gpd.GeoDataFrame(polygon_df, geometry='polygon')
    polygon_gdf.crs = {'init': 'epsg:4326'}

    ax = polygon_gdf.geometry.plot(linewidth=2.0, color='red', edgecolor='red', alpha=0.5)
    mplleaflet.show(fig=ax.figure, tiles='cartodb_positron')


def run():
    if os.path.exists('data/cluster_points.csv'):
        cluster_points = pd.read_csv('data/cluster_points.csv')
    else:
        lagos_data = load_data()
        lagos_data = cleanup_dataframe(lagos_data)
        lagos_data = create_radian_columns(lagos_data)
        lagos_data['Cluster'] = cluster_accidents(lagos_data, eps_in_meters=1000)

        # Filter out the noise points and retain only the clusters
        cluster_points = lagos_data.loc[lagos_data['Cluster'] > -1, ['Cluster', 'Latitude', 'Longitude']]

        # Write the cluster points to a csv file
        cluster_points.to_csv("data/cluster_points.csv", index=False)

    # Display the blob map
    show_blob_map(cluster_points)

    # Display the concave hull map
    show_concave_hull_map(cluster_points)


if __name__ == "__main__":
    run()