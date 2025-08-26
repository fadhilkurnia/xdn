import itertools
import threading
import multiprocessing
from utils import cluster_locations
from utils import get_server_locations
from utils import get_estimated_rtt_latency
from utils import get_per_city_clients
from utils import get_client_count_per_city
from utils import get_population_ratio_per_city
from replica_group_picker import find_k_closest_servers
from replica_group_picker import get_client_locations

population_data_file = "location_distributions/client_us_metro_population.csv"
server_edge_location_file = "location_distributions/server_netflix_oca_us.csv"

geolocality = 0
num_replicas = 2
num_clients = 100
c_lat_slowdown_factor = 0.32258064516

# prepare the clients
city_locations = get_client_locations(population_data_file)
population_ratio_per_city = get_population_ratio_per_city(city_locations)
client_count_per_city = get_client_count_per_city(population_ratio_per_city, 
                                                  num_clients, 
                                                  geolocality, 
                                                  city_locations[0]['City'])
clients = get_per_city_clients(client_count_per_city, city_locations)

# prepare the servers
nf_edge_server_locations = get_server_locations([server_edge_location_file], 
                                                remove_duplicate_location=True,
                                                remove_nonredundant_city_servers=True)

server_by_name = {}
for server in nf_edge_server_locations:
    server_by_name[server['Name']] = server

# calculate the heuristic server location
print("> Calculating the heuristic solution ... ")
replica_name_set = set()
replicas = []

# cluster clients into num_replicas group
print(f">> clustering clients into {num_replicas} groups ... ")
client_location_list = []
for client in clients:
    for i in range(client['Count']):
        client_location_list.append((client['Latitude'], client['Longitude']))
_, centroids = cluster_locations(client_location_list, num_replicas)

# find a replica for each group, this should be with d-tree to be optimal
for center in centroids:
    ref_server = {'Name': 'ReferenceServer', 'Latitude': center[0], 'Longitude': center[1]}
    closest_servers = find_k_closest_servers(nf_edge_server_locations, ref_server, 1)
    assert len(closest_servers) == 1
    replica_name_set.add(closest_servers[0]['Name'])
    replicas.append(closest_servers[0])
assert len(replica_name_set) == num_replicas, f"We need more read only server in this case: len={len(replica_name_set)} num_replicas={num_replicas}"

# assign client to the closest servers
for i, client in enumerate(clients):
    client_lat = client['Latitude']
    client_lon = client['Longitude']
    closest_servers = find_k_closest_servers(replicas, {"Latitude": client_lat, "Longitude": client_lon}, 1)
    assert len(closest_servers) == 1
    clients[i]['TargetReplicaName'] = closest_servers[0]['Name']

# calculate the wighted latency across clients
sum_weighted_lat = 0
sum_client_demand = 0
for client in clients:
    client_demand = client['Count']
    client_lat = client['Latitude']
    client_lon = client['Longitude']
    server = server_by_name[client['TargetReplicaName']]
    server_lat = server['Latitude']
    server_lon = server['Longitude']
    client_server_rtt_latency = get_estimated_rtt_latency((client_lat, client_lon), 
                                                            (server_lat, server_lon), 
                                                            c_lat_slowdown_factor)
    sum_weighted_lat += client_demand * client_server_rtt_latency
    sum_client_demand += client_demand

print("Summary:")
print("- The heuristic score: ", sum_weighted_lat)
print("- Average latency    : ", sum_weighted_lat / float(sum_client_demand))
print("- Heuristic replicas : ", replicas)
print("")
print("")

print("> Calculating the optimal solutions ... ")
print(">> Number of server options: ", len(nf_edge_server_locations))
min_sum = float('inf')
min_sum_demand = float('inf')
min_replica_group = None
sever_indices = range(0, len(nf_edge_server_locations))

# prepare runner threads
thread_results = []
def calculate_score(thread_id, curr_combination_servers, clients):

    # assign client to the closest servers
    for i, client in enumerate(clients):
        client_lat = client['Latitude']
        client_lon = client['Longitude']
        closest_servers = find_k_closest_servers(curr_combination_servers, {"Latitude": client_lat, "Longitude": client_lon}, 1)
        assert len(closest_servers) == 1
        clients[i][f"T{thread_id}-TargetReplicaName"] = closest_servers[0]['Name']

    # sum the weighted latency across clients
    sum_weighted_lat = 0
    sum_client_demand = 0
    for client in clients:
        client_demand = client['Count']
        client_lat = client['Latitude']
        client_lon = client['Longitude']
        server = server_by_name[client[f"T{thread_id}-TargetReplicaName"]]
        server_lat = server['Latitude']
        server_lon = server['Longitude']
        client_server_rtt_latency = get_estimated_rtt_latency((client_lat, client_lon), 
                                                              (server_lat, server_lon), 
                                                              c_lat_slowdown_factor)
        sum_weighted_lat += client_demand * client_server_rtt_latency
        sum_client_demand += client_demand

    thread_results.append((sum_weighted_lat, sum_client_demand, curr_combination_servers))

curr_thread_id = 0
threads = []
for comb in itertools.combinations(sever_indices, num_replicas):    
    # prepare the selected servers
    curr_combination_servers = []
    for idx in comb:
        curr_combination_servers.append(nf_edge_server_locations[idx])

    runner = threading.Thread(target=calculate_score, args=(curr_thread_id, curr_combination_servers, clients, ))
    runner.start()
    threads.append(runner)
    
    curr_thread_id += 1

for t in threads:
    t.join()
for result in thread_results:
    sum_weighted_lat = result[0]
    sum_client_demand = result[1]
    curr_combination_servers = result[2]
    if sum_weighted_lat < min_sum:
        min_sum = sum_weighted_lat
        min_replica_group = curr_combination_servers
        min_sum_demand = sum_client_demand

print("Summary:")
print("- The minimum score: ", min_sum)
print("- Average latency  : ", min_sum / float(sum_client_demand))
print("- Optimal replicas : ", min_replica_group)
print("")
print("")
