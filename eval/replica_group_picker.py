#!/usr/bin/env python3

import os
import csv
import math
import heapq
import statistics
from collections import Counter
import matplotlib.pyplot as plt

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

    # Final distance in meters
    distance = R * c

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
        
        total_weight += count
        weighted_lat_sum += lat * count
        weighted_lon_sum += lon * count
    
    # Compute the weighted average (centroid)
    centroid_lat = weighted_lat_sum / total_weight
    centroid_lon = weighted_lon_sum / total_weight
    
    return centroid_lat, centroid_lon

def find_k_closest_servers(servers, reference_server, k):
    """
    Find the k closest servers to server x in arr using a max-heap approach.
    
    :param servers: List of servers, each containing:
        {
            "Name": str,
            "Latitude": float,
            "Longitude": float
        }
    :param reference_server: The target server we measure distance against.
    :param k: The number of closest servers to find.
    :return: List of the k closest servers, ordered by distance. Example:
        {
            "Name": str,
            "Latitude": float,
            "Longitude": float,
            "Distance": float
        }
    """
    if k <= 0:
        return []

    # If k >= length of arr, simply return all elements
    if k >= len(servers):
        for i, s in enumerate(servers):
            distance = get_distance((reference_server["Latitude"], 
                                     reference_server["Longitude"]),
                                    (s["Latitude"], s["Longitude"]))
            servers[i]["Distance"] = distance
        return sorted(servers, key=lambda d: d['Distance'])
    
    # We'll maintain a max-heap of size k.
    # In Python, heapq implements a min-heap, so we'll store negative distances
    # to simulate a max-heap.
    max_heap = []

    leader_lat = reference_server["Latitude"]
    leader_lon = reference_server["Longitude"]

    for value in servers:
        server_lat = value["Latitude"]
        server_lon = value["Longitude"]
        distance =  get_distance((leader_lat, leader_lon), (server_lat, server_lon))
        value["Distance"] = distance
        
        # Push a tuple (negative_distance, value)
        value_tuples = [(k, v) for k, v in value.items()] 
        heapq.heappush(max_heap, (-distance, value_tuples))
        
        # If the heap size exceeds k, pop the element with the largest distance (smallest negative_distance)
        if len(max_heap) > k:
            heapq.heappop(max_heap)
    
    # The heap now contains k elements with the smallest distances.
    # Extract the values (second item in the tuple) from the heap
    closest_values = [dict(item[1]) for item in max_heap]

    closest_values = sorted(closest_values, key=lambda d: d['Distance'])
    return closest_values

def get_server_locations(server_location_filenames):
    server_names = set()
    servers = []
    for filename in server_location_filenames:
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                server_name = row["Name"]
                if server_name in server_names:
                    raise Exception(f"Found duplicate server name of {server_name}")
                else:
                    server_names.add(server_name)
                servers.append({
                    "Name": server_name,
                    "Latitude": float(row["Latitude"]),
                    "Longitude": float(row["Longitude"]),
                })
    return servers

def get_client_locations(client_location_filename, num_clients = None):
    """
    Find the city client from an external csv file. The expected header is:
        city,location,population
    Population is in the form of "latitude;longitude" or "NA;NA" if unknown.
    
    :param client_location_filename: csv filename containing city client location.
    :param num_clients: Optional number to adjust the number of clients.
    :return: List of the clients with adjusted count, if num_clients was set.
    """
    parsed_cities = set()
    clients = []

    # parse clients, ignore clients with unknown location
    with open(client_location_filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            location = row["location"].strip()
            if location == "NA;NA":
                continue
            parts = location.split(";")
            latitude = float(parts[0])
            longitude = float(parts[1])
            
            city_name = row["city"]
            if city_name in parsed_cities:
                raise Exception(f"Found duplicate city of {city_name}")
            else:
                parsed_cities.add(city_name)
            
            clients.append({
                "City": city_name,
                "Count": int(row["population"]),
                "Latitude": latitude,
                "Longitude": longitude,
            })

    # sort clients by their counts
    sorted(clients, key=lambda d: d['Count'])

    # adjust the number of client proportionally based on the
    # number of clients specified.
    if num_clients != None:
        assert num_clients > 0
        
        total_num_client = 0
        for c in clients:
            total_num_client += c['Count']

        num_clients_left = num_clients
        for i, c in enumerate(clients):
            if num_clients_left <= 0:
                clients[i]['Count'] = 0
            
            curr_count = c['Count']
            adjusted_num_client = float(curr_count) / float(total_num_client) * num_clients
            adjusted_num_client = max(round(adjusted_num_client), 1)
            if num_clients_left - adjusted_num_client < 0:
                adjusted_num_client = num_clients_left
            clients[i]['Count'] = adjusted_num_client
            num_clients_left -= adjusted_num_client

        first_idx_zero_cnt = len(clients)
        for i, c in enumerate(clients):
            if c['Count'] == 0:
                first_idx_zero_cnt = i
                break
        clients = clients[:first_idx_zero_cnt]
        
        total_num_client = 0
        for c in clients:
            total_num_client += c['Count']
        assert num_clients == total_num_client

    return clients

def get_heuristic_replicas_placement(server_locations, client_locations, 
                                     num_replicas, is_silent = False):
    """
    Find the replica group placement based on the server and client location
    distribution, assuming leader-based replica coordination.
    
    :param server_locations: List of servers, each containing:
        {
            "Name": str,
            "Latitude": float,
            "Longitude": float
        }
    :param client_locations: A list of dictionaries, each containing:
        {
            "City": str,
            "Count": float or int,
            "Latitude": float,
            "Longitude": float
        }
    :param reference_server: The target server we measure distance against.
    :return: A dictionary containing:
        {
            "Centroid": tuple of (latitude, longitude),
            "Leader": a server,
            "Replicas": list of server
        }
    """
    assert len(server_locations) > 0
    assert len(client_locations) > 0
    assert num_replicas > 0

    # Get total number of clients
    total_num_client = 0
    for c in client_locations:
        total_num_client += c['Count']
    assert total_num_client > 0

    # Find centroid of demands
    centroid_lat, centroid_lon = calculate_centroid(client_locations)

    # Find the closest location to the centroid as the leader/coordinator
    dummy_reference_server = {'Name': 'Centroid', 
                              'Latitude': centroid_lat,
                              'Longitude': centroid_lon}
    leader_candidates = find_k_closest_servers(server_locations, dummy_reference_server, 1)
    assert len(leader_candidates) > 0
    leader = leader_candidates[0]

    # Find the remaining replicas, including the leader
    closest_replicas = find_k_closest_servers(server_locations, leader, num_replicas)
    assert len(closest_replicas) > 0

    if not is_silent:
        print("Heuristic-based placement result")
        print("#clients\t:", total_num_client)
        print("centroid\t:", centroid_lat, centroid_lon)
        print("leader\t\t:", leader)
        print("replicas\t:")
        for r in closest_replicas:
            print(" - ", r)

    return {"Centroid": (centroid_lat, centroid_lon),
            "Leader": leader,
            "Replicas": closest_replicas}

def get_optimal_replicas_placement(server_locations, client_locations, 
                                   num_replicas):
    # optimal via brute force
    curr_group = server_locations[:num_replicas]
    curr_leader = curr_group[0]
    optimal_solution = {'Leader': curr_leader, "Replicas": curr_group}
    min_score = float('inf')

    for leader_candidate in curr_group:
        curr_leader = leader_candidate

        curr_score = 0
        
        # assign client to closest replica in the current group

        # calculate distance per each replica


    

    return

def get_estimated_rtt_latency(coord1, coord2, c_slowdown_factor):
    _SPEED_OF_LIGHT_M_S = 299_792_458.0 # Spped of light in m/s
    
    estimated_dist_m = get_distance(coord1, coord2)
    estimated_tx_speed_m_s = _SPEED_OF_LIGHT_M_S * c_slowdown_factor
    estimated_tx_lat_s = estimated_dist_m / estimated_tx_speed_m_s
    estimated_tx_lat_ms = estimated_tx_lat_s * 1_000.0      # convert to ms
    estimated_rtt_lat_ms = estimated_tx_lat_ms * 2          # double for rtt
    
    return estimated_rtt_lat_ms

def get_latency_expectation(replica_locations, client_locations, leader_name, 
                            lat_slowdown_factor):
    assert len(replica_locations) > 0
    assert len(client_locations) > 0
    assert lat_slowdown_factor > 0 and lat_slowdown_factor <= 1
    replica_names = set()
    for r in replica_locations:
        replica_names.add(r['Name'])
    assert leader_name in replica_names

    _EXECUTION_LAT_MS = 3
    _LCL_COOR_LAT_OVERHEAD_MS = 2
    _MIN_LCL_RTT_LAT_MS = 1

    replicas = replica_locations
    leader = None
    for r in replicas:
        if r['Name'] == leader_name:
            leader = r
            break
    assert leader != None
    
    # Calculate the estimated coordination latency for the leader.
    # For leader as the entry replica: leader -> quorum -> leader.
    # Get RTT to the closest quorum, that is the latency to the furthest
    # server in the closest quorum. Note that find_k_closest_servers
    # returns servers ordered by distance.
    quorum_size = (len(replicas)+1) // 2
    closest_peers = find_k_closest_servers(replicas, leader, quorum_size+1)
    assert len(closest_peers) >= 1
    furthest_quorum_server = closest_peers[-1]
    expected_quorum_rtt_lat_ms = get_estimated_rtt_latency(
                    (furthest_quorum_server["Latitude"], 
                     furthest_quorum_server["Longitude"]), 
                    (leader["Latitude"], leader["Longitude"]),
                    lat_slowdown_factor)
    # handle edge case for local coordination
    estimated_coor_lat_ms = max(expected_quorum_rtt_lat_ms, _MIN_LCL_RTT_LAT_MS) + _LCL_COOR_LAT_OVERHEAD_MS
    estimated_coor_exec_lat_ms = estimated_coor_lat_ms + _EXECUTION_LAT_MS
    leader['Latency'] = estimated_coor_exec_lat_ms

    
    # Iterates over the non-leader entry replicas, for each get the expected 
    # execution latency, considering the leader-based coordination.
    for r in replicas:

        # for leader as the entry replica, we did the calculation earliear.
        if r["Name"] == leader["Name"]:
            r['Latency'] = leader['Latency']
            continue

        # for non-leader as the entry replica: entry -> leader -> quorum -> leader -> entry
        if r["Name"] != leader["Name"]:
            entry_leader_rtt_lat_ms = get_estimated_rtt_latency(
                    (leader["Latitude"], 
                     leader["Longitude"]), 
                    (r["Latitude"], r["Longitude"]),
                    lat_slowdown_factor)
            estimated_coor_lat_ms = entry_leader_rtt_lat_ms + leader['Latency']
            estimated_coor_exec_lat_ms = estimated_coor_lat_ms + _EXECUTION_LAT_MS
            r['Latency'] = estimated_coor_exec_lat_ms
            continue

        raise Exception("Unknown replica type")

    # Iterates over the client, for each client get the closest replica
    # TODO: move to separate function in the xdn_measure_latency.py file.
    for i, c in enumerate(client_locations):
        dummy_reference = {'Name': c["City"], 'Latitude': c["Latitude"], 'Longitude': c["Longitude"]}
        closest_replica = find_k_closest_servers(replicas, dummy_reference, 1)
        assert len(closest_replica) == 1
        client_locations[i]["TargetReplica"] = closest_replica[0]['Name']

    replica_by_name = {}
    for r in replicas:
        replica_by_name[r["Name"]] = r

    # Iterate over the clients, gather the expected latencies
    replica_by_name = {}
    for r in replicas:
        replica_by_name[r["Name"]] = r
    latencies = []
    for c in client_locations:
        target_entry_replica_name = c["TargetReplica"]
        target_entry_replica = replica_by_name[target_entry_replica_name]
        expected_rtt_lat_ms = get_estimated_rtt_latency(
            (c["Latitude"], c["Longitude"]),
            (target_entry_replica["Latitude"], target_entry_replica["Longitude"]),
            lat_slowdown_factor
        )
        expected_e2e_lat_ms = target_entry_replica["Latency"] + expected_rtt_lat_ms
        for i in range(c["Count"]):
            latencies.append(expected_e2e_lat_ms)

    return latencies
    
def get_latency_expectation_for_cd_ed(frontend_locations, datastore_coordinate, 
                                      client_locations, c_lat_slowdown_factor):
    assert len(frontend_locations) > 0
    assert c_lat_slowdown_factor > 0 and c_lat_slowdown_factor <= 1

    _EXECUTION_LAT_MS = 3
    
    # get processing latency for each frontend
    proc_lat_by_srv = {}
    fe_by_name = {}
    db_lat = datastore_coordinate[0]
    db_lon = datastore_coordinate[1]
    for fe in frontend_locations:
        fe_db_rtt_lat_ms = get_estimated_rtt_latency(
            (fe["Latitude"], fe["Longitude"]),
            (db_lat, db_lon),
            c_lat_slowdown_factor)
        proc_lat_by_srv[fe["Name"]] = fe_db_rtt_lat_ms
        fe_by_name[fe["Name"]] = fe

    # Iterates over the client, for each client get the closest frontend
    # TODO: move to separate function in the xdn_measure_latency.py file.
    for i, c in enumerate(client_locations):
        dummy_reference = {'Name': c["City"], 'Latitude': c["Latitude"], 'Longitude': c["Longitude"]}
        closest_replica = find_k_closest_servers(frontend_locations, dummy_reference, 1)
        assert len(closest_replica) == 1
        client_locations[i]["TargetReplica"] = closest_replica[0]['Name']


    # Iterates over the clients, get the expected latencies
    latencies = []
    for c in client_locations:
        target_fe = c["TargetReplica"]
        fe_srv = fe_by_name[target_fe]
        expected_client_fe_rtt_lat_ms = get_estimated_rtt_latency(
            (c["Latitude"], c["Longitude"]),
            (fe_srv["Latitude"], fe_srv["Longitude"]),
            c_lat_slowdown_factor
        )
        expected_e2e_lat_ms = proc_lat_by_srv[target_fe] + expected_client_fe_rtt_lat_ms
        for i in range(c["Count"]):
            latencies.append(expected_e2e_lat_ms)

    return latencies

def get_latency_expectation_for_gd(frontend_locations, datastore_location, c_lat_slowdown_factor):
    latencies = []
    return latencies

def printout_lat_stats(latencies, title = None):
    # Calculate and display statistics
    avg_latency = statistics.mean(latencies)
    med_latency = statistics.median(latencies)
    variance = statistics.variance(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)
    print("")
    if title == None:
        print(">> Expectation:")
    else:
        print(f">> {title}")
    print(f"Number of successful requests: {len(latencies)}")
    print(f"Expected average latency: {avg_latency:.4f} ms")
    print(f"Expected median latency: {med_latency:.4f} ms")
    print(f"Expected minimum latency: {min_latency:.4f} ms")
    print(f"Expected maximum latency: {max_latency:.4f} ms")
    print(f"Expected variance: {variance:.4f}")
    return

def get_latency_expectation_by_approaches(server_locations, client_locations, 
                                          num_replicas, c_lat_slowdown_factor):
    # 1. XDN's heuristic based approach
    replica_group = get_heuristic_replicas_placement(server_locations, client_locations, num_replicas)
    leader = replica_group["Leader"]
    replicas = replica_group["Replicas"]
    xdn_latencies = get_latency_expectation(replicas, client_locations, leader['Name'], c_lat_slowdown_factor)
    printout_lat_stats(xdn_latencies, "Expectation with XDN's heuristic-based placement:")

    # 2. No Replication approach
    # server_name = "iad"
    server_name = "iad-1-c004"
    replica_group = []
    for s in server_locations:
        if s['Name'] == server_name:
            replica_group.append(s)
            break
    assert len(replica_group) == 1
    nr_latencies = get_latency_expectation(replica_group, client_locations, server_name, c_lat_slowdown_factor)
    printout_lat_stats(nr_latencies, f"Expectation with NoReplication at {server_name}:")

    # TODO: optimal placement
    
    plot_frequency_multiple([xdn_latencies, nr_latencies], 
                            ["XDN", "No Replication"])
    return

def plot_frequency_multiple(latency_lists, labels=None):
    """
    Plots individual bar charts (one per list of latencies) arranged vertically.
    
    :param latency_lists: list of lists, each containing latencies (floats in ms).
    :param labels: optional list of labels (strings) for each latency list.
    """
    if labels is None:
        labels = [f"Dataset {i+1}" for i in range(len(latency_lists))]

    # ----------------------------------------------------------------------
    # 1) Determine the global max frequency (y-axis) and global max bin (x-axis)
    # ----------------------------------------------------------------------
    global_max_freq = 0
    global_max_bin = 0
    counters = []
    
    for latencies in latency_lists:
        latencies_ms = [int(math.floor(x)) for x in latencies]
        counts = Counter(latencies_ms)
        counters.append(counts)
        
        # Check local max frequency
        local_max_freq = max(counts.values()) if counts else 0
        if local_max_freq > global_max_freq:
            global_max_freq = local_max_freq
        
        # Check local max bin
        local_max_bin = max(counts.keys()) if counts else 0
        if local_max_bin > global_max_bin:
            global_max_bin = local_max_bin
    
    # Provide a small margin for top of y-axis
    if global_max_freq > 0:
        y_max = global_max_freq * 1.1
    else:
        y_max = 1.0

    # Provide a small margin for right of x-axis
    # (You can adjust the factor or offset to suit your needs.)
    if global_max_bin > 0:
        x_max = global_max_bin * 1.005
    else:
        x_max = 1.0
    
    # ----------------------------------------------------------------------
    # 2) Create subplots (stacked vertically)
    # ----------------------------------------------------------------------
    fig, axes = plt.subplots(
        nrows=len(latency_lists), 
        ncols=1, 
        figsize=(8, 3 * len(latency_lists)),
        sharex=False,  # Change to True if you want all x-axes to share the same range
        sharey=False   # Change to True if you want all y-axes to share the same range
    )
    
    # If there's only one latency list, 'axes' won't be a list; make it a list for uniform iteration.
    if len(latency_lists) == 1:
        axes = [axes]
    
    # ----------------------------------------------------------------------
    # 3) Plot each dataset in a separate subplot
    # ----------------------------------------------------------------------
    for idx, latencies in enumerate(latency_lists):
        ax = axes[idx]
        
        # Convert latencies to integer bins and count
        latencies_ms = [int(math.floor(x)) for x in latencies]
        counts = Counter(latencies_ms)
        
        # Prepare x-values (unique latencies sorted) and y-values (their frequencies)
        x_vals = sorted(counts.keys())
        y_vals = [counts[x] for x in x_vals]
        
        # Plot the bar chart for this list
        ax.bar(x_vals, y_vals, width=0.9, align='center', edgecolor='black')

        # Compute Mean and Median
        mean_val = statistics.mean(latencies_ms)
        median_val = statistics.median(latencies_ms)
        
        # Plot vertical lines for mean (red, dashed) and median (green, dash-dot)
        ax.axvline(
            mean_val, 
            color='red', 
            linestyle='--', 
            linewidth=2, 
            label=f'Mean = {mean_val:.2f} ms'
        )
        ax.axvline(
            median_val,
            color='green',
            linestyle='-.',
            linewidth=2,
            label=f'Median = {median_val:.2f} ms'
        )
        
        # Axis labels and title
        ax.set_xlim(left=0, right=x_max)
        ax.set_ylim(bottom=0, top=y_max)
        ax.set_xlabel('Latency (ms)')
        ax.set_ylabel('Frequency')
        ax.set_title(labels[idx])
        ax.legend(loc='best')
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="...")
    parser.add_argument("server_location", help="Server location csv file")
    parser.add_argument("demand_location", help="Demand location csv file")
    parser.add_argument("-r", "--num-replicas", type=int, default=3, help="Number of replica")
    parser.add_argument("-m", "--mode", type=str, default="picker", help="'picker' or 'expectation'")
    parser.add_argument("-c", "--num-clients", type=int, default=1000, help="Number of clients for calculating expectation")
    parser.add_argument("-s", "--slowdown-factor", type=float, default=0.31, help="Speed of light slowdown factor for calculating expectation")
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
        print(f"Demand location file:{args.demand_location} cannot be found.")
        exit(-1)
    expected_columns=["city", "location", "population"]
    with open(args.demand_location, 'r') as f:
        first_line = f.readline().strip()
        columns = set(first_line.split(","))
        for c in expected_columns:
            if c not in columns:
                print(f"Expecting column '{c}' in the demand location file.")
                exit(-1)

    # read servers and clients into list of dict
    servers = get_server_locations(server_location_files)
    clients = get_client_locations(args.demand_location, args.num_clients)

    if args.mode == "picker":
        get_heuristic_replicas_placement(servers, clients, args.num_replicas)
    elif args.mode == "expectation":
        get_latency_expectation_by_approaches(servers, clients, args.num_replicas, args.slowdown_factor)
    else:
        raise(f"Unknown mode {args.mode}")