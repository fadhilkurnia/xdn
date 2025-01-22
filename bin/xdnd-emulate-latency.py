# xdnd-emulate-latency.py
#
# Script to emulate latency based on the geolocation data provided in the
# gigapaxos properties file. We emulate latency by injecting it using tcconfig.

import math
import subprocess

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points on the Earth 
    using the haversine formula. Returns distance in kilometers.
    """
    # Earth radius in kilometers
    R = 6371.0

    # Convert degrees to radians
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = ((math.sin(dphi / 2) ** 2) 
         + math.cos(phi1) * math.cos(phi2) * (math.sin(dlambda / 2) ** 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Distance in kilometers
    distance = R * c
    return distance

def read_servers_from_property_file(filename: str) -> dict:
    """
    Reads a property file with lines like:
      active.aa="192.168.1.10"
      active.aa.geolocation="39.5832,-82.3511"
    and returns a dictionary of the form:
      {
         "aa": {
             "ip": "192.168.1.10",
             "geolocation": (39.5832, -82.3511)
         },
         ...
      }
    """
    servers = {}
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            line = line.partition('#')[0]

            # Skip empty lines or commented lines if any
            if not line or line.startswith("#"):
                continue

            # Only consider lines for server location or host
            if not line.startswith("active"):
                continue

            # Handle config for server location.
            # Each line should look like active.xx.geolocation="lat,lon"
            if "geolocation" in line:
                parts = line.split("=")
                
                # Skip malformed lines
                if len(parts) != 2:
                    continue

                key = parts[0].strip()
                value = parts[1].strip().strip('"')

                # Skip malformed key
                key_parts = key.split('.')
                if len(key_parts) < 3:
                    continue

                # Get server name, e.g., 'aa' from 'active.aa.geolocation'
                server_name = key_parts[1]

                # value is something like '39.5832,-82.3511'
                # split by comma to get lat & lon
                coords = value.split(',')
                if len(coords) != 2:
                    continue

                lat_str, lon_str = coords
                try:
                    lat = float(lat_str)
                    lon = float(lon_str)
                    if server_name not in servers:
                        servers[server_name] = {}
                    servers[server_name]["geolocation"] = (lat, lon)
                except ValueError:
                    # Skip if not valid floats
                    continue

            # Handle config for server host
            # Example line: 'active.node1=10.10.1.3:2000'
            parts = line.split("=")
            if len(parts) == 2 and len(parts[0].split(".")) == 2:
                key_parts = parts[0].split(".")
                server_name = key_parts[1]

                # get the host, assuming it as IP address
                host_parts = parts[1].split(":")
                host = host_parts[0]
                
                if server_name not in servers:
                    servers[server_name] = {}
                servers[server_name]["host"] = host
    
    return servers

def get_latency_slowdown_from_property_file(filename: str) -> float:
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            line = line.partition('#')[0]

            # Skip empty lines or commented lines if any
            if not line or line.startswith("#"):
                continue
            
            if line.startswith("XDN_EVAL_LATENCY_SLOWDOWN_FACTOR"):
                parts = line.split('=')
                if len(parts) < 2:
                    continue
                parts = parts[1].strip().split()
                slowdown_str = parts[0]
                slowdown = float(slowdown_str)
                return slowdown
    return 1.0

def get_estimated_latency(distance_km, slowdown_factor):
    """
    Estimate the one-way latency (in milliseconds) for a signal traveling 
    'distance_km' kilometers, taking into account the speed of light and 
    a 'slowdown_factor'.

    :param distance_km:       The distance in kilometers (float).
    :param slowdown_factor:   A factor (0 < slowdown_factor <= 1) indicating 
                              how much slower than the speed of light 
                              the signal travels.
    :return:                  Estimated latency in milliseconds (float).

    Example:
        >>> # Suppose a distance of 1000 km, with a slowdown factor of 0.1
        >>> # This means the signal travels at 10% the speed of light
        >>> latency_ms = estimate_latency(1000, 0.8)
        >>> print(latency_ms, "ms")
    """

    # Validate slowdown factor
    if not (0 < slowdown_factor <= 1):
        raise ValueError("Slowdown factor must be between 0 and 1 (exclusive of 0, inclusive of 1).")

    # Speed of light in km/s
    speed_of_light_km_s = 299792.458

    # Time in seconds = distance / (speed of light * slowdown factor)
    time_seconds = distance_km / (speed_of_light_km_s * slowdown_factor)

    # Convert to milliseconds
    time_milliseconds = time_seconds * 1000

    return time_milliseconds

def inject_server_latency(servers, slowdown):
    server_names = list(servers.keys())

    # reseting the injected latency
    for i in range(len(server_names)):
        s1 = server_names[i]
        host = servers[s1]["host"]
        cmd = f"ssh fadhil@{host} sudo tcdel eth1 --all"
        print(">> " + cmd)
        rcode = subprocess.call(cmd, shell=True)
        print("   " + str(rcode))
    
    # injecting the latency
    for i in range(len(server_names)):
        for j in range(i + 1, len(server_names)):
            s1 = server_names[i]
            s2 = server_names[j]
            lat1, lon1 = servers[s1]["geolocation"]
            lat2, lon2 = servers[s2]["geolocation"]
            distance_km = haversine_distance(lat1, lon1, lat2, lon2)
            latency_ms = get_estimated_latency(distance_km, slowdown)

            src_network = servers[s1]["host"]
            dst_network = servers[s2]["host"]
            
            cmd = f"ssh fadhil@{src_network} sudo tcset eth1 --delay {latency_ms}ms --network {dst_network} --add"
            print(">> " + cmd)
            rcode = subprocess.call(cmd, shell=True)
            print("   " + str(rcode))

    return

def main():
    property_file = 'conf/gigapaxos.cloudlab-virtual.properties'
    
    # Read all servers' geolocations and IP
    servers = read_servers_from_property_file(property_file)

    # Read slowdown factor from the config file
    slowdown = get_latency_slowdown_from_property_file(property_file)
    
    # Get a list of server names so we can pair them
    server_names = list(servers.keys())

    # Printout all server names and host
    print("All the registered servers:")
    for name in server_names:
        print(" >> " + name + " " + servers[name]["host"])
    print("Slowdown factor: " + str(slowdown))
    
    # Calculate distances and latency between all unique pairs
    print("\nDistance and latency between server pairs:")
    for i in range(len(server_names)):
        for j in range(i + 1, len(server_names)):
            s1 = server_names[i]
            s2 = server_names[j]
            lat1, lon1 = servers[s1]["geolocation"]
            lat2, lon2 = servers[s2]["geolocation"]
            distance_km = haversine_distance(lat1, lon1, lat2, lon2)
            latency_ms = get_estimated_latency(distance_km, slowdown)
            print(f" >> {s1} <-> {s2}: {distance_km:.2f} km \t ({latency_ms} ms)")

    print("\nInjecting emulated latency:")
    inject_server_latency(servers, slowdown)

if __name__ == "__main__":
    main()