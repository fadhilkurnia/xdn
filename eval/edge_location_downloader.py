import os
import requests
from datetime import datetime
import threading
import urllib3

urllib3.disable_warnings()

urls = [
    {"name": "fed_all", "url": "https://analytics.usa.gov/data/live/top-cities-realtime.csv"},
    {"name": "fed_treasury", "url": "https://analytics.usa.gov/data/treasury/top-cities-realtime.csv"},
    {"name": "fed_usps", "url": "https://analytics.usa.gov/data/postal-service/top-cities-realtime.csv"},
    {"name": "fed_commerce", "url": "https://analytics.usa.gov/data/commerce/top-cities-realtime.csv"},
    {"name": "fed_health", "url": "https://analytics.usa.gov/data/health-human-services/top-cities-realtime.csv"},
    {"name": "fed_justice", "url": "https://analytics.usa.gov/data/justice/top-cities-realtime.csv"},
    {"name": "fed_transport", "url": "https://analytics.usa.gov/data/transportation/top-cities-realtime.csv"},
    {"name": "fed_nist", "url": "https://analytics.usa.gov/data/national-institute-standards-technology/top-cities-realtime.csv"},
    {"name": "fed_noaa", "url": "https://analytics.usa.gov/data/national-oceanic-atmospheric-administration/top-cities-realtime.csv"},
    {"name": "fed_energy", "url": "https://analytics.usa.gov/data/energy/top-cities-realtime.csv"},
    {"name": "fed_nasa", "url": "https://analytics.usa.gov/data/national-aeronautics-space-administration/top-cities-realtime.csv"},
    {"name": "fed_veteran", "url": "https://analytics.usa.gov/data/veterans-affairs/top-cities-realtime.csv"},
    {"name": "fed_fbi", "url": "https://analytics.usa.gov/data/federal-bureau-investigation/top-cities-realtime.csv"},
    {"name": "fed_ftc", "url": "https://analytics.usa.gov/data/federal-trade-commission/top-cities-realtime.csv"},
    {"name": "fed_nsf", "url": "https://analytics.usa.gov/data/national-science-foundation/top-cities-realtime.csv"},
    {"name": "fed_ssa", "url": "https://analytics.usa.gov/data/social-security-administration/top-cities-realtime.csv"},
]

def download_client_location_data(url, filename):
    print(f">> downloading '{filename}' from {url}.")
    response = requests.get(url, verify=False)
    with open(filename, 'wb') as f:
        f.write(response.content)

directory = "downloads"
if not os.path.exists(directory):
    os.makedirs(directory)

current_time = datetime.now()
current_time = current_time.strftime("%Y%m%d%H%M%S")
for data in urls:
    name=data["name"]
    url=data["url"]
    target_filename=f"downloads/{name}_{current_time}_client_locations.csv"
    t = threading.Thread(target=download_client_location_data, args=(url, target_filename))
    t.start()
