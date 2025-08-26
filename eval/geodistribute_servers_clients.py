# STEP-1: Client distribution
#   1. Select metro from client distribution file
#   2. Select geo-locality factor
#   3. Distribute local client inside the metro area
#   4. Distribute clients outside the metro area, proportional to the population.
#      - For each metro, distribute clients uniformly in the metro area.
# 
# STEP-2: Placing replicas
#   1. For XDN with linearizability 
#         - get "center of mass".
#         - pick replica for leader
#         - pick replicas for follower
#   2. For NR
#         - put all in us-east-1
#   3. For CD
#         - group the clients into 3 clusters
#         - pick frontend replicas in each of the 3 clusters
#         - put datastore in us-east-1
#   4. For GD
#         - group the clients into 3 clusters
#         - pick frontend replicas in each of the 3 clusters
#         - put datastore in 3 cloud regions
#   5. For ED
#         - group the clients into 3 clusters
#         - pick frontend replicas in each of the 3 clusters
#         - get "center of mass".
#         - pick server for durable object instance

import statistics
import numpy as np
from replica_group_picker import get_client_locations
from replica_group_picker import get_heuristic_replicas_placement
from replica_group_picker import get_latency_expectation
from utils import get_spanner_placement_menu
from utils import get_population_ratio_per_city
from utils import get_client_count_per_city
from utils import get_uniform_client_per_city
from utils import get_per_city_clients
from utils import get_expected_latencies
from utils import get_server_locations

# define constant variables
population_data_file = "location_distributions/client_us_metro_population.csv"
population_data_file = "location_distributions/client_world_metro_population.csv"
# population_data_file = "location_distributions/client_world_metro_population_old.csv"
server_edge_location_file = "location_distributions/server_netflix_oca.csv"
server_cf_edge_location_file = "location_distributions/server_cloudflare.csv"
server_aws_region_location_file = "location_distributions/server_aws_region.csv"
server_gcp_region_location_file = "location_distributions/server_gcp_region.csv"

geolocalities = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]
# geolocalities = [0.2]
approaches = ["XDN", "XDNNR", "CD", "GD", "ED"]
city_area_sqkm = 1000
num_clients = 1000
num_replicas = 3
# static_nr_server = 'us-east-1'   # Tokyo
static_nr_server = 'ap-northeast-1'   # Tokyo
c_lat_slowdown_factor = 0.32258064516
is_keep_duplicate_edge_server_location=True
is_remove_non_redundant_city=True
is_sample_city=True
is_spread_city_client=False

print(">> is_sample_city ", is_sample_city)

# read and parse servers
cf_edge_server_locations = get_server_locations([server_cf_edge_location_file])
aws_region_servers = get_server_locations([server_aws_region_location_file])
gcp_region_servers = get_server_locations([server_gcp_region_location_file])
edge_server_locations = get_server_locations([server_edge_location_file], 
                                             remove_duplicate_location=(not is_keep_duplicate_edge_server_location),
                                             remove_nonredundant_city_servers=is_remove_non_redundant_city)

# provide replica group options for spanner
spanner_placement_menu_info = get_spanner_placement_menu(gcp_region_servers)
spanner_placement_menu = spanner_placement_menu_info["PlacementMenu"]
spanner_placement_menu_per_leader = spanner_placement_menu_info["PlacementMenuByLeader"]
gcp_server_by_name = {}
for server in gcp_region_servers:
    gcp_server_by_name[server['Name']] = server
spanner_leader_locations = []
for leader_name, menu in spanner_placement_menu_per_leader.items():
    server = gcp_server_by_name[leader_name]
    spanner_leader_locations.append(server.copy())

print(spanner_placement_menu_info)
exit(1)

# add spanner leader to edge consideration
for leader in spanner_leader_locations:
    server = gcp_server_by_name[leader['Name']]
    copied_server_a = server.copy()
    copied_server_a['Name'] += "_a"
    copied_server_b = server.copy()
    copied_server_b['Name'] += "_b"
    edge_server_locations.append(copied_server_a)
    edge_server_locations.append(copied_server_b)
# for menu_name, replica_name_list in spanner_placement_menu_info['PlacementMenu'].items():
#     for replica_name in replica_name_list:
#         server = gcp_server_by_name[replica_name]
#         copied_server = server.copy()
#         edge_server_locations.append(server.copy())

# read and parse clients
city_locations = get_client_locations(population_data_file)
sampled_city_names = set()
if is_sample_city:
    sampled_ratio = 0.12
    num_sample = round(len(city_locations) * sampled_ratio)
    num_sample_batch = num_sample // 3
    print(f">> num sample: {num_sample}")
    print(f">> num sampled batch: {num_sample_batch}")
    first_batch = city_locations[:num_sample_batch]
    list_length = len(city_locations)
    middle_start = (list_length - num_sample_batch) // 2
    middle_batch = city_locations[middle_start:middle_start + num_sample_batch]
    last_batch = city_locations[-num_sample_batch:]
    sampled_cities = first_batch + middle_batch + last_batch
    for c in sampled_cities:
        sampled_city_names.add(c['City'])
population_ratio_per_city = get_population_ratio_per_city(city_locations)
city_by_name = {}
for city in city_locations:
    city_by_name[city['City']] = city

for geolocality in geolocalities:
    # prepare containers for expectation results
    all_city_latencies_by_approach = {}
    for approach in approaches:
        all_city_latencies_by_approach[approach] = []
    
    # iterate over 50 local areas (i.e., city)
    for picked_city in city_by_name.keys():

        if is_sample_city:
            if picked_city not in sampled_city_names:
                continue

        assert geolocality >= 0.0 and geolocality <= 1.0

        # STEP-1: Distributing clients based on geolocality
        # assigning local and non-local clients
        local_city_name = picked_city
        client_count_per_city = get_client_count_per_city(population_ratio_per_city, num_clients, geolocality, local_city_name)
        clients = []
        if is_spread_city_client:
            clients = get_uniform_client_per_city(client_count_per_city, city_locations, city_area_sqkm)
        else:
            clients = get_per_city_clients(client_count_per_city, city_locations)

        # STEP-2: Choosing where to put our replicas, for each approach
        for approach in approaches:
            # Case-1: XDN
            if approach == "XDN":
                replica_group_info = get_heuristic_replicas_placement(edge_server_locations, clients, num_replicas, is_silent=True)
                latencies = get_expected_latencies(replica_group_info['Replicas'], 
                                                   clients, 
                                                   replica_group_info['Leader']['Name'],
                                                   c_lat_slowdown_factor)
                all_city_latencies_by_approach[approach].extend(latencies)

                # if picked_city == "Tokyo" and geolocality == 0.2:
                #     per_city_latencies = get_expected_latencies(replica_group_info['Replicas'], 
                #                                                 clients, 
                #                                                 replica_group_info['Leader']['Name'], 
                #                                                 c_lat_slowdown_factor,
                #                                                 is_report_per_city=True)
                #     print(">>>>>>> leader: " + replica_group_info['Leader']['Name'])
                #     for client_city_name, pc_latencies in per_city_latencies.items():
                #         print(f">>>>>>> {client_city_name:15}:  {statistics.mean(pc_latencies):.3f}ms")

            # Case-2: XDN without replication
            elif approach == "XDNNR":
                replica_group_info = get_heuristic_replicas_placement(edge_server_locations, clients, 1, is_silent=True)
                latencies = get_expected_latencies(replica_group_info["Replicas"], 
                                                   clients, 
                                                   replica_group_info["Leader"]["Name"], 
                                                   c_lat_slowdown_factor)
                all_city_latencies_by_approach[approach].extend(latencies)

            # Case-3: CD
            elif approach == "CD":
                replica_group = []
                for s in aws_region_servers:
                    if s['Name'] == static_nr_server:
                        replica_group.append(s)
                        break
                latencies = get_expected_latencies(replica_group, clients, replica_group[0]["Name"], c_lat_slowdown_factor)
                all_city_latencies_by_approach[approach].extend(latencies)

            # Case-4: GD
            elif approach == "GD":
                # use spanner specified location to chose optimal leader
                # e.g., ['us-central1', 'us-central1', 'us-east4']
                replica_group_info = get_heuristic_replicas_placement(spanner_leader_locations, clients, 1, is_silent=True)
                placement_menu_name = spanner_placement_menu_per_leader[replica_group_info['Leader']['Name']]
                replica_group_member = spanner_placement_menu[placement_menu_name]
                replica_group = []
                for member in replica_group_member:
                    replica_group.append(gcp_server_by_name[member])
                
                # replica_group_info = get_heuristic_replicas_placement(aws_region_servers, clients, 2, is_silent=True)
                # replica_group = replica_group_info['Replicas']
                # replica_group.append(replica_group_info['Leader'])
                
                # print(f">> GD: {picked_city:15} replicas={replica_group}")
                # print(f">> GD: {picked_city:15} leader  ={replica_group_info['Leader']['Name']}")

                latencies = get_expected_latencies(replica_group, 
                                                   clients, 
                                                   replica_group_info['Leader']['Name'], 
                                                   c_lat_slowdown_factor)
                all_city_latencies_by_approach[approach].extend(latencies)

                # if picked_city == "Tokyo" and geolocality == 0.2:
                #     per_city_latencies = get_expected_latencies(replica_group, 
                #                                                 clients, 
                #                                                 replica_group_info['Leader']['Name'], 
                #                                                 c_lat_slowdown_factor,
                #                                                 is_report_per_city=True)
                #     print(">>>>>>> leader: " + replica_group_info['Leader']['Name'])
                #     for client_city_name, pc_latencies in per_city_latencies.items():
                #         print(f">>>>>>> {client_city_name:15}:  {statistics.mean(pc_latencies):.3f}ms")
                

                # latencies = np.array(latencies)
                # avg_latency = statistics.mean(latencies)
                # med_latency = statistics.median(latencies)
                # var_latency = statistics.variance(latencies)
                # min_latency = min(latencies)
                # max_latency = max(latencies)
                # p90_latency = np.percentile(latencies, 90)
                # p95_latency = np.percentile(latencies, 95)
                # p99_latency = np.percentile(latencies, 99)
                # print(f">>> {approach}    g={geolocality} city={local_city_name:20}: avg={avg_latency:6.2f}ms var={var_latency:8.2f} | min={min_latency:6.2f}ms max={max_latency:6.2f}ms | p50={med_latency:6.2f}ms p90={p90_latency:6.2f}ms p95={p95_latency:6.2f}ms p99={p99_latency:6.2f}ms")

            # Case-5: ED
            elif approach == "ED":
                # get the edge server for the edge datastore
                replica_group = get_heuristic_replicas_placement(cf_edge_server_locations, clients, 1, is_silent=True)
                latencies = get_latency_expectation(replica_group["Replicas"], 
                                                    clients, 
                                                    replica_group["Leader"]["Name"], 
                                                    c_lat_slowdown_factor)
                all_city_latencies_by_approach[approach].extend(latencies)

            else:
                raise Exception(f"Unknown approach: {approach}")


    import matplotlib.pyplot as plt

    def plot_latency_distribution(approach, geolocality, latencies, min_latency, max_latency):
        """
        Plots a histogram of latency distribution using 1 ms bins.
        
        Parameters
        ----------
        latencies : list or array-like
            A list of latency values in milliseconds.
        min_latency : int
            The lower bound (inclusive) for the latency range shown on the x-axis.
        max_latency : int
            The upper bound (inclusive) for the latency range shown on the x-axis.
        """
        # Filter the latencies to the specified range (optional if you only want to zoom in)
        # latencies_in_range = [lat for lat in latencies if min_latency <= lat <= max_latency]
        # If you want to show all data but only zoom the plot, comment out the above line 
        # and change hist range parameter below or axis limits.

        # Compute the average (mean) latency
        mean_latency = np.mean(latencies)

        # Create 1-ms bin edges from min_latency to max_latency
        bins = range(min_latency, max_latency + 2)  # +1 for the upper edge, +1 to include that edge

        plt.figure()
        plt.hist(latencies, bins=bins, edgecolor='black')  # edgecolor for visual clarity
        
        # Draw the vertical line for mean latency
        plt.axvline(mean_latency, linestyle='--', linewidth=1)
        
        # Get the current y-axis max to position the text
        _, max_ylim = plt.ylim()

        # Place the text slightly to the right and near the top of the histogram
        plt.text(mean_latency + 0.2, max_ylim * 0.9, f"Mean: {mean_latency:.2f} ms")
        
        plt.xlabel("Latency (ms)")
        plt.ylabel("Frequency")
        plt.title(f"Latency Distribution of {approach} with g={geolocality}")
        
        # Limit the x-axis to the specified range
        plt.xlim(min_latency, max_latency + 1)
        
        plt.show()
    
    def printout_stats(approach, latencies):
        if len(latencies) == 0:
            return
        latencies = np.array(latencies)
        avg_latency = statistics.mean(latencies)
        med_latency = statistics.median(latencies)
        var_latency = statistics.variance(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p90_latency = np.percentile(latencies, 90)
        p95_latency = np.percentile(latencies, 95)
        p99_latency = np.percentile(latencies, 99)
        print(f'>> Approach={approach:5}\t g={geolocality}\t avg={avg_latency:6.2f}ms var={var_latency:8.2f} | min={min_latency:6.2f}ms max={max_latency:6.2f}ms | p50={med_latency:6.2f}ms p90={p90_latency:6.2f}ms p95={p95_latency:6.2f}ms p99={p99_latency:6.2f}ms')
        # plot_latency_distribution(approach, geolocality, latencies, 0, 90)
    
    approaches = list(approaches)
    approaches.sort(reverse=True)
    for approach in approaches:
        printout_stats(approach, all_city_latencies_by_approach[approach])
    print()


# TODO: make plot!