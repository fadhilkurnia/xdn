import csv
import math
import heapq
import random
import numpy as np
from sklearn.cluster import KMeans

random_seed=313
random.seed(random_seed)

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

def get_estimated_rtt_latency(coord1, coord2, c_slowdown_factor):
    _SPEED_OF_LIGHT_M_S = 299_792_458.0 # Spped of light in m/s
    
    estimated_dist_m = get_distance(coord1, coord2)
    estimated_tx_speed_m_s = _SPEED_OF_LIGHT_M_S * c_slowdown_factor
    estimated_tx_lat_s = estimated_dist_m / estimated_tx_speed_m_s
    estimated_tx_lat_ms = estimated_tx_lat_s * 1_000.0      # convert to ms
    estimated_rtt_lat_ms = estimated_tx_lat_ms * 2          # double for rtt
    
    return estimated_rtt_lat_ms

def get_spanner_placement_menu(gcp_region_server_locations):
    gcp_server_by_name={}
    for server in gcp_region_server_locations:
        gcp_server_by_name[server['Name']] = server
    spanner_og_placement_menu = {
        'asia1': ['asia-northeast1', 'asia-northeast1', 'asia-northeast2'],
        'asia2': ['asia-south1', 'asia-south1', 'asia-south2'],
        'eur3': ['europe-west1', 'europe-west1', 'europe-west4'],
        'eur5': ['europe-west2', 'europe-west2', 'europe-west1'],
        'eur6': ['europe-west4', 'europe-west4', 'europe-west3'],
        'eur7': ['europe-west8', 'europe-west8', 'europe-west3'],
        'nam3': ['us-east4', 'us-east4', 'us-east1'],
        'nam6': ['us-central1', 'us-central1', 'us-east1'],
        'nam7': ['us-central1', 'us-central1', 'us-east4'],
        'nam8': ['us-west2', 'us-west2', 'us-west1'],
        'nam9': ['us-east4', 'us-east4', 'us-central1'],
        'nam10': ['us-central1', 'us-central1', 'us-west3'],
        'nam11': ['us-central1', 'us-central1', 'us-east1'],
        'nam12': ['us-central1', 'us-central1', 'us-east4'],
        'nam13': ['us-central2', 'us-central2', 'us-central1'],
        'nam14': ['us-east4', 'us-east4', 'northamerica-northeast1'],
        'nam15': ['us-south1', 'us-south1', 'us-east4'],
        'nam16': ['us-central1', 'us-central1', 'us-east4'],
        'nam-eur-asia1': ['us-central1', 'us-central1', 'us-central2'],
        'nam-eur-asia1': ['us-central1', 'us-central1', 'us-east1'],
    }
    gcp_nam_unique_leader_servers = set()
    for menu, rg in spanner_og_placement_menu.items():
        gcp_nam_unique_leader_servers.add(rg[0])
    gcp_nam_leader_servers = []
    for leader in gcp_nam_unique_leader_servers:
        gcp_nam_leader_servers.append(gcp_server_by_name[leader])
    # having menu with the same leader, pick one with the lowest coordination latency
    spanner_rg_by_leader = {}
    for menu, rg in spanner_og_placement_menu.items():
        if rg[0] in spanner_rg_by_leader:
            spanner_rg_by_leader[rg[0]].append(menu)
        else:
            spanner_rg_by_leader[rg[0]] = [menu]
    spanner_placement_menu = {}
    spanner_placement_menu_by_leader = {}
    for leader, menu in spanner_rg_by_leader.items():
        min_coor_rtt_ms = float('inf')
        min_menu = None
        for rg_name in menu:
            srv1_lat = gcp_server_by_name[leader]['Latitude']
            srv1_lon = gcp_server_by_name[leader]['Longitude']
            srv2_lat = gcp_server_by_name[spanner_og_placement_menu[rg_name][-1]]['Latitude']
            srv2_lon = gcp_server_by_name[spanner_og_placement_menu[rg_name][-1]]['Longitude']
            expected_lat_ms = get_estimated_rtt_latency((srv1_lat, srv1_lon), (srv2_lat, srv2_lon), 1.0)
            if expected_lat_ms < min_coor_rtt_ms:
                min_coor_rtt_ms = expected_lat_ms
                min_menu = rg_name
        spanner_placement_menu[min_menu] = spanner_og_placement_menu[min_menu]
        spanner_placement_menu_by_leader[leader] = min_menu
    
    return {"PlacementMenu": spanner_placement_menu, "PlacementMenuByLeader": spanner_placement_menu_by_leader}

def get_server_locations(server_location_filenames, 
                         remove_duplicate_location=False, 
                         remove_nonredundant_city_servers=False):
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
                    "City": row['City'],
                    "Country": row['Country'],
                    "Latitude": float(row["Latitude"]),
                    "Longitude": float(row["Longitude"]),
                })
    
    # remove servers with redundant location
    if remove_duplicate_location:
        visited_locations = set()
        unique_server_locations = []
        for s in servers:
            curr_location = f"{s['Latitude']}:{s['Longitude']}"
            if curr_location in visited_locations:
                continue
            visited_locations.add(curr_location)
            unique_server_locations.append(s)
        servers = unique_server_locations

    # remove server location with only one server available
    if remove_nonredundant_city_servers:
        server_count_per_city = {}
        for s in servers:
            if s['City'] not in server_count_per_city:
                server_count_per_city[s['City']] = 0
            server_count_per_city[s['City']] += 1
        redundant_server_locations = []
        for s in servers:
            if server_count_per_city[s['City']] > 1:
                redundant_server_locations.append(s)
        servers = redundant_server_locations

    return servers

def generate_uniform_points_in_circle(center_lat_deg, center_lon_deg, radius_km, N):
    """
    Generate N points uniformly distributed on Earth's surface
    inside a circle of given radius (in km) around a center lat/lon.
    
    Parameters:
    -----------
    center_lat_deg : float
        Center latitude in degrees.
    center_lon_deg : float
        Center longitude in degrees.
    radius_km      : float
        Circle radius in kilometers.
    N              : int
        Number of random points to generate.

    Returns:
    --------
    points : list of tuples
        List of (latitude, longitude) in degrees for the N points.
    """
    # Earth’s mean radius in km (approx.)
    EARTH_RADIUS = 6371.0
    
    # Convert center lat/lon to radians
    lat0 = math.radians(center_lat_deg)
    lon0 = math.radians(center_lon_deg)
    
    # Maximum central angle for this circle
    alpha = radius_km / EARTH_RADIUS

    points = []
    for _ in range(N):
        # 1) Pick a random fraction u for area-based sampling
        u = random.random()
        
        # 2) Determine the colatitude from the center of the circle
        colat = math.acos(1 - u * (1 - math.cos(alpha)))

        # 3) Pick a random bearing from 0 to 2*pi
        bearing = 2 * math.pi * random.random()

        # 4) Compute the new latitude
        lat = math.asin(
            math.sin(lat0) * math.cos(colat) +
            math.cos(lat0) * math.sin(colat) * math.cos(bearing)
        )
        
        # 5) Compute the difference in longitude
        dlon = math.atan2(
            math.sin(bearing) * math.sin(colat) * math.cos(lat0),
            math.cos(colat) - math.sin(lat0) * math.sin(lat)
        )
        
        # 6) Final longitude
        lon = lon0 + dlon
        
        # Normalize longitude to be between -pi and pi
        lon = (lon + 3 * math.pi) % (2 * math.pi) - math.pi
        
        # Convert back to degrees
        lat_deg = math.degrees(lat)
        lon_deg = math.degrees(lon)

        points.append((lat_deg, lon_deg))

    return points

def cluster_locations(locations, k):
    """
    Cluster a list of latitude/longitude pairs into k clusters using K-Means.
    
    Parameters:
    -----------
    locations : list of tuples
        A list of (latitude, longitude) pairs, e.g. [(lat1, lon1), (lat2, lon2), ...].
    k : int
        The number of clusters to form.

    Returns:
    --------
    labels : ndarray of shape (n_samples,)
        Cluster labels for each location (from 0 to k-1).
    centroids : ndarray of shape (k, 2)
        Coordinates of cluster centers as (latitude, longitude).
    """
    # Convert list of (lat, lon) to a NumPy array of shape (n_samples, 2)
    X = np.array(locations)

    # Initialize and fit the KMeans model
    kmeans = KMeans(n_clusters=k, random_state=random_seed)
    kmeans.fit(X)

    # Retrieve cluster labels and cluster centers
    labels = kmeans.labels_
    centroids = kmeans.cluster_centers_

    return labels, centroids

def get_population_ratio_per_city(city_locations):
    """
    Returns a map of city name to population ratio in that city
    """
    population_ratio_per_city = {}
    sum_population = 0
    for city in city_locations:
        sum_population += city['Count']
    for city in city_locations:
        population_ratio_per_city[city['City']] = float(city['Count']) / sum_population
    return population_ratio_per_city

def get_client_count_per_city(population_ratio_per_city, total_clients, 
                              geolocality, local_city_name):
    """
    Given population ratio, geolocality, and local city name, returns the number
    of client per city in a dictionary/map.
    """
    assert geolocality >= 0.0 and geolocality <= 1.0
    assert total_clients > 0
    assert local_city_name in population_ratio_per_city.keys()

    # calculates the number of clients for local and non-local cities
    local_city_pop_ratio = population_ratio_per_city[local_city_name]
    num_local_clients = max(geolocality, local_city_pop_ratio) * total_clients
    num_nonlocal_clients = total_clients - num_local_clients

    # recalibrating population ratio for the non-local cities
    sum_nonlocal_ratio = 0.0
    for city_name, ratio in population_ratio_per_city.items():
        if city_name == local_city_name:
            continue
        sum_nonlocal_ratio += ratio
    nonlocal_pop_ratio = {}
    for city_name, ratio in population_ratio_per_city.items():
        if city_name == local_city_name:
            continue
        nonlocal_pop_ratio[city_name] = ratio / sum_nonlocal_ratio

    # distributing local and non local clients
    client_count_per_city = {}
    running_total_clients = 0
    for city_name in population_ratio_per_city.keys():
        # assign local clients
        if city_name == local_city_name:
            assigned_num_clients = math.ceil(num_local_clients)
            if running_total_clients + assigned_num_clients > total_clients:
                assigned_num_clients = total_clients - running_total_clients
            client_count_per_city[city_name] = assigned_num_clients
            running_total_clients += assigned_num_clients
            continue
        # assign non-local clients
        assigned_num_clients = math.ceil(nonlocal_pop_ratio[city_name] * num_nonlocal_clients)
        if running_total_clients + assigned_num_clients > total_clients:
            assigned_num_clients = total_clients - running_total_clients
        client_count_per_city[city_name] = assigned_num_clients
        running_total_clients += assigned_num_clients

    return client_count_per_city

def get_uniform_client_per_city(client_count_per_city, city_locations, 
                                city_area_sqkm):
    assert type(client_count_per_city) == dict
    assert type(city_locations) == list
    assert type(city_area_sqkm) == int
    # maps city location by name
    city_by_name = {}
    for city in city_locations:
        city_by_name[city['City']] = city

    # distribute the client in each city, assuming the city is a circle
    clients = []
    city_radius_km = math.sqrt(city_area_sqkm / math.pi)
    curr_client_id = 0
    for city_name, num_city_clients in client_count_per_city.items():
        city_lat = float(city_by_name[city_name]['Latitude'])
        city_lon = float(city_by_name[city_name]['Longitude'])
        client_locations = generate_uniform_points_in_circle(city_lat, 
                                                             city_lon, 
                                                             city_radius_km, 
                                                             num_city_clients)
        for i in range(num_city_clients):
            curr_client = {
                'ClientID': curr_client_id, 
                'City': city_name, 
                'Count': 1, 
                'Latitude': client_locations[i][0],
                'Longitude': client_locations[i][1]
            }
            clients.append(curr_client)
            curr_client_id += 1
    
    return clients

def get_per_city_clients(client_count_per_city, city_locations):
    """
    This is the same as the `get_uniform_client_per_city` function, but we dont 
    spread the client in each city.
    """
    # maps city location by name
    city_by_name = {}
    for city in city_locations:
        city_by_name[city['City']] = city

    # distribute the client in each city
    clients = []
    curr_client_id = 0
    for city_name, num_city_clients in client_count_per_city.items():
        if num_city_clients == 0:
            continue
        city_lat = float(city_by_name[city_name]['Latitude'])
        city_lon = float(city_by_name[city_name]['Longitude'])
        
        curr_client = {
            'ClientID': curr_client_id, 
            'City': city_name, 
            'Count': num_city_clients, 
            'Latitude': city_lat,
            'Longitude': city_lon
        }
        clients.append(curr_client)
        curr_client_id += 1
    
    return clients

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

def get_expected_latencies(replica_locations, client_locations, leader_name, 
                           lat_slowdown_factor, is_report_per_city=False,
                           is_direct_all_to_leader=False):
    """
    Given a replica group placement and client spatial distribution, this 
    function calculates the expected latencies.
    """
    assert len(replica_locations) > 0
    assert len(client_locations) > 0
    assert lat_slowdown_factor > 0 and lat_slowdown_factor <= 1
    replica_names = set()
    for r in replica_locations:
        replica_names.add(r['Name'])
    assert leader_name in replica_names

    _EXECUTION_LAT_MS = 2
    _LCL_COOR_LAT_OVERHEAD_MS = 1.5

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
    # returns servers ordered by distance (ascending).
    quorum_size = (len(replicas)+1) // 2
    closest_peers = find_k_closest_servers(replicas, leader, quorum_size)
    assert len(closest_peers) >= 1
    furthest_quorum_server = closest_peers[-1]
    expected_quorum_rtt_lat_ms = get_estimated_rtt_latency(
                    (furthest_quorum_server["Latitude"], 
                     furthest_quorum_server["Longitude"]), 
                    (leader["Latitude"], leader["Longitude"]),
                    lat_slowdown_factor)
    # handle edge case for local coordination
    estimated_coor_lat_ms = expected_quorum_rtt_lat_ms + _LCL_COOR_LAT_OVERHEAD_MS
    estimated_coor_exec_lat_ms = estimated_coor_lat_ms + _EXECUTION_LAT_MS
    leader['Latency'] = estimated_coor_exec_lat_ms

    
    # Iterates over the non-leader entry replicas, for each get the expected 
    # execution latency, considering the leader-based coordination.
    for r in replicas:

        # for leader as the entry replica, we did the calculation already.
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
    for i, c in enumerate(client_locations):
        closest_replica = []
        if not is_direct_all_to_leader:
            dummy_client_reference = {'Name': c["City"], 
                                    'Latitude': c["Latitude"], 
                                    'Longitude': c["Longitude"]}
            closest_replica = find_k_closest_servers(replicas, dummy_client_reference, 1)
        if is_direct_all_to_leader:
            closest_replica = [leader]
        assert len(closest_replica) == 1, f"size={len(closest_replica)}: {closest_replica}"
        client_locations[i]["TargetReplica"] = closest_replica[0]['Name']

    replica_by_name = {}
    for r in replicas:
        replica_by_name[r["Name"]] = r

    # Iterate over the clients, gather the expected latencies
    replica_by_name = {}
    for r in replicas:
        replica_by_name[r["Name"]] = r
    latencies = []
    latencies_per_city = {}
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

        if is_report_per_city:
            city_name = c['City']
            if city_name not in latencies_per_city:
                latencies_per_city[city_name] = []
            for i in range(c['Count']):
                latencies_per_city[city_name].append(expected_e2e_lat_ms)

    if is_report_per_city:
        return latencies_per_city

    return latencies