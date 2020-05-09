from cliff.api import Cliff


cliff_server_addr = "http://149.165.168.205:8080"

#ref: https://pypi.org/project/mediacloud-cliff/
def extract_locaiton_info(text):
    my_cliff = Cliff(cliff_server_addr)
    print(my_cliff.parse_text(text))
    print(my_cliff.geonames_lookup(4943351))


tweet = "This is about Einstien at the IIT in New Delhi."

extract_locaiton_info(tweet)
