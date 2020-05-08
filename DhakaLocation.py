import googlemaps
from datetime import datetime

gmaps = googlemaps.Client(key='AIzaSyAes-DiwwsHDVVRyppeEwF2qFy3rMD7NaA')

#Geocoding an address
geocode_result = gmaps.geocode('Adabor, Dhaka, Bangladesh')


print (geocode_result)


