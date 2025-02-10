import os
import csv
import math
import heapq

def get_distance(coord1, coord2):
    """
    Calculate the great-circle distance between two points on Earth (in meters)
    using the Haversine formula.

    :param coord1: Tuple (lat1, lon1) in degrees.
    :param coord2: Tuple (lat2, lon2) in degrees.
    :return: Distance in meters (float).
    """
    # Approximate radius of Earth in meters
    R = 6371000  

    lat1, lon1 = coord1
    lat2, lon2 = coord2

    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Differences in coordinates
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Haversine formula
    a = (math.sin(dlat / 2) ** 2
         + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c  # Final distance in meters

    return distance



def calculate_centroid(demand_locations):
    """
    Calculate the weighted centroid (latitude, longitude) of a list of demand locations.
    
    :param demand_locations: A list of dictionaries, each containing:
        {
            "City": str,
            "Count": float or int,
            "Latitude": float,
            "Longitude": float
        }
    :return: A tuple (centroid_latitude, centroid_longitude)
    """
    if not demand_locations:
        # If the list is empty, return None or raise an exception
        return None
    
    total_weight = 0.0
    weighted_lat_sum = 0.0
    weighted_lon_sum = 0.0

    # Sum up the weights and multiply coordinates by their respective weight
    for location in demand_locations:
        count = location["Count"]
        lat = location["Latitude"]
        lon = location["Longitude"]
        # count *= 1000
        
        total_weight += count
        weighted_lat_sum += lat * count
        weighted_lon_sum += lon * count
    
    # Compute the weighted average (centroid)
    centroid_lat = weighted_lat_sum / total_weight
    centroid_lon = weighted_lon_sum / total_weight
    
    return centroid_lat, centroid_lon

def find_k_closest_servers(arr, x, k):
    """
    Find the k closest servers to server x in arr using a max-heap approach.
    
    :param arr: List of servers.
    :param x: The target server we measure distance against.
    :param k: The number of closest servers to find.
    :return: List of the k closest servers.
    """
    if k <= 0:
        return []

    # If k >= length of arr, simply return all elements
    if k >= len(arr):
        return arr
    
    # We'll maintain a max-heap of size k
    # In Python, heapq implements a min-heap, so we'll store negative distances
    # to simulate a max-heap.
    max_heap = []

    leader_lat = x["Latitude"]
    leader_lon = x["Longitude"]

    for value in arr:
        server_lat = value["Latitude"]
        server_lon = value["Longitude"]
        distance =  get_distance((leader_lat, leader_lon), (server_lat, server_lon))
        
        # Push a tuple (negative_distance, value)
        heapq.heappush(max_heap, (-distance, value))
        
        # If the heap size exceeds k, pop the element with the largest distance (smallest negative_distance)
        if len(max_heap) > k:
            heapq.heappop(max_heap)
    
    # The heap now contains k elements with the smallest distances.
    # Extract the values (second item in the tuple) from the heap
    closest_values = [item[1] for item in max_heap]
    
    return closest_values



def main(server_locations, demand_location, num_replica):
    # validate num_replica
    if num_replica <= 0 or num_replica > 100:
        print("Number of replica must be between 0 (exclusive) and 100 (inclusive)")
        exit(-1)

    # parse server locations
    servers = []
    for filename in server_locations:
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                servers.append({
                    "Name": row["Name"],
                    "Latitude": float(row["Latitude"]),
                    "Longitude": float(row["Longitude"]),
                })

    # parse clients demand location
    clients = []
    sum_count = 0
    with open(demand_location, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            location = row["location"].strip()
            if location == "NA;NA":
                continue
            parts = location.split(";")
            latitude = float(parts[0])
            longitude = float(parts[1])
            clients.append({
                "City": row["city"],
                "Count": int(row["population"]),
                "Latitude": latitude,
                "Longitude": longitude,
            })
            sum_count += int(row["population"])

    # Find centroid of demands
    centroid_lat, centroid_lon = calculate_centroid(clients)

    # Find closest location to the centroid as the coordinator
    min_distance = float('inf')
    closest_server = None
    for s in servers:
        dist = get_distance((centroid_lat, centroid_lon), (s["Latitude"], s["Longitude"]))
        if dist < min_distance:
            min_distance = dist
            closest_server = s

    # Find closest replicas to the coordinator (using)
    leader = closest_server
    closest_replicas = find_k_closest_servers(servers, leader, num_replica)

    print("Optimal placement result")
    print("#clients\t:", sum_count)
    print("centroid\t:", centroid_lat, centroid_lon)
    print("leader\t\t:", leader)
    print("replicas\t:")
    for r in closest_replicas:
        print(" - ", r, " distance: ", get_distance((r["Latitude"], r["Longitude"]),(leader["Latitude"], leader["Longitude"])))

    return

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="...")
    parser.add_argument("server_location", help="Server location csv file")
    parser.add_argument("demand_location", help="Demand location csv file")
    parser.add_argument("-r", "--num-replica", type=int, default=3, help="Number of replica")
    args = parser.parse_args()

    # validate server location files (comma separated)
    server_location_files = args.server_location
    server_location_files = server_location_files.split(",")
    for filename in server_location_files:
        if not os.path.exists(filename):
            print(f"Server location file:{filename} cannot be found.")
            exit(-1)
        expected_first_line="Name,City,Country,Latitude,Longitude"
        with open(filename, 'r') as f:
            first_line = f.readline().strip()
            if first_line != expected_first_line:
                print(f"Unexpected file format for {filename}, expecting .csv file with Name, City, Country, Latitude, and Longitude columns.")
                exit(-1)

    # validate demand location file
    if not os.path.exists(args.demand_location):
        print(f"Demand location file:{filename} cannot be found.")
        exit(-1)
    expected_columns=["city", "location", "population"]
    with open(args.demand_location, 'r') as f:
        first_line = f.readline().strip()
        columns = set(first_line.split(","))
        for c in expected_columns:
            if c not in columns:
                print(f"Expecting column '{c}' in the demand location file.")
                exit(-1)

    main(server_location_files, args.demand_location, args.num_replica)