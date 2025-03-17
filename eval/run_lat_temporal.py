# read clients
# split clients into 4 timezone
# create client distribution for every 30 minutes following the sinusoidal distribution

import copy
import statistics
import numpy as np
import matplotlib.pyplot as plt
from replica_group_picker import get_client_locations
from replica_group_picker import get_heuristic_replicas_placement
from utils import get_server_locations
from utils import get_expected_latencies
from utils import get_spanner_placement_menu

def school_hours_distribution(t, mean_local=12, std_hours=2.0, tz_offset=0):
    """
    Returns a Gaussian-like distribution restricted to [8, 14] local time.
    Outside [8, 14], distribution is zero.

    Parameters
    ----------
    t          : array-like, hours in UTC
    mean_local : float, center (local hours) of the Gaussian peak (default=12)
    std_hours  : float, standard deviation in hours
    tz_offset  : float, time zone offset from UTC (e.g., -5 for US East Standard Time)

    Returns
    -------
    dist       : array-like, distribution values for each t
    """
    # Convert UTC time into local time for this time zone
    local_time = (t - tz_offset) % 24

    # Gaussian shape, with peak at mean_local
    dist = np.exp(-0.5 * ((local_time - mean_local) / std_hours)**2)

    # Zero out values outside the [8, 14] window (school hours)
    mask = (local_time < 8) | (local_time > 14)
    dist[mask] = 0

    # Optionally normalize so each distribution has a maximum of 1
    # (only if there’s at least one nonzero value)
    maxval = dist.max()
    if maxval > 0:
        dist /= maxval

    return dist

population_data_file = "location_distributions/client_us_metro_population.csv"
server_edge_location_file = "location_distributions/server_netflix_oca.csv"
server_aws_region_location_file = "location_distributions/server_aws_region.csv"
server_gcp_region_location_file = "location_distributions/server_gcp_region.csv"

max_num_clients = 1000
num_replicas = 3
c_lat_slowdown_factor = 0.32258064516
static_nr_server = 'us-east-1'
is_print_client_temporal_dist=False
approaches = ['XDN', 'CD', 'ED', 'GD']

city_locations = get_client_locations(population_data_file)
nf_edge_server_locations = get_server_locations([server_edge_location_file], remove_duplicate_location=True)
aws_region_servers = get_server_locations([server_aws_region_location_file])
gcp_region_servers = get_server_locations([server_gcp_region_location_file])

# prepare static location for CD
cd_replica_group = []
for s in aws_region_servers:
    if s['Name'] == static_nr_server:
        cd_replica_group.append(s)
        break

# provide replica group options for multi-region datastore (e.g., spanner)
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

# get the timezone
timezones = set()
for city in city_locations:
    if city['Timezone'] != "":
        timezones.add(city['Timezone'])
print(">>> timezone:", timezones)

# group cities by timezone
city_list_by_timezone = {}
for city in city_locations:
    timezone = city['Timezone']
    if timezone not in city_list_by_timezone:
        city_list_by_timezone[timezone] = []
    city_list_by_timezone[timezone].append(city)
# calculate per timezone population ratio
for timezone in timezones:
    city_list = city_list_by_timezone[timezone]
    sum_population = 0
    for city in city_list:
        sum_population += city['Count']
    for i, city in enumerate(city_list):
        city_list_by_timezone[timezone][i]['PerTzPopulationRatio'] = float(city['Count']) / float(sum_population)

# generate num of cients distributions in each timestamp and timezone, every 60 mins
timestamps = np.arange(13, 20, 1)
offsets_utc = {
    "Eastern":    5,   # UTC-5
    "Central":    6,   # UTC-6
    "Mountain":   7,   # UTC-7
    "Pacific":    8    # UTC-8
}
client_tz_count_by_timestamp = {}
num_et_clients = school_hours_distribution(timestamps, 10, 0.4, offsets_utc['Eastern']) * max_num_clients
num_ct_clients = school_hours_distribution(timestamps, 10, 0.4, offsets_utc['Central']) * max_num_clients
num_mt_clients = school_hours_distribution(timestamps, 10, 0.4, offsets_utc['Mountain']) * max_num_clients
num_pt_clients = school_hours_distribution(timestamps, 10, 0.4, offsets_utc['Pacific']) * max_num_clients
for idx, timestamp in enumerate(timestamps):
    num_et_client = int(round(num_et_clients[idx]))
    num_ct_client = int(round(num_ct_clients[idx]))
    num_mt_client = int(round(num_mt_clients[idx]))
    num_pt_client = int(round(num_pt_clients[idx]))
    print(f'>>> timestamp_utc={timestamp} |\t et={num_et_client:7d}\t ct={num_ct_client:7d}\t mt={num_mt_client:7d}\t pt={num_pt_client:7d}')
    client_tz_count_by_timestamp[timestamp] = {'Eastern': num_et_client, 
                                               'Central': num_ct_client,
                                               'Mountain': num_mt_client,
                                               'Pacific': num_pt_client}

# distribute the clients spatially, for each timestamp and timezone
client_list_by_timestamp = {}
client_list_by_timestamp_timezone = {}
for timestamp in timestamps:
    client_list_by_timestamp[timestamp] = []
    client_list_by_timestamp_timezone[timestamp] = {}
    for timezone in timezones:
        city_list = city_list_by_timezone[timezone]
        client_list_by_timestamp_timezone[timestamp][timezone] = copy.deepcopy(city_list)
        tz_sum_client = client_tz_count_by_timestamp[timestamp][timezone]
        for city in client_list_by_timestamp_timezone[timestamp][timezone]:
            city['Count'] = int(round(tz_sum_client * city['PerTzPopulationRatio']))
        client_list_by_timestamp[timestamp].extend(client_list_by_timestamp_timezone[timestamp][timezone])

# printout the number of client in each city at each timestamp
if is_print_client_temporal_dist:
    for timestamp in timestamps:
        clients = client_list_by_timestamp[timestamp]
        print(f">>>> timestamp {timestamp}")
        for client in clients:
            if client['Count'] != 0:
                city_name = client["City"].replace(" ", "")
                city_count = client['Count']
                print(f'     city={city_name:12}\t cnt={city_count}')

print()
print()

# ==============================================================================
#                           Expectation Calculation
# ==============================================================================

# do numerical latency measurement for each approach
raw_result = []
for approach in approaches:
    tz_latencies_per_city = {}
    required_servers = set()        # capture the required servers to be emulated
    active_cities = set()           # capture all cities with clients

    ed_replica_group_info = None
    
    for timestamp in timestamps:
        clients = client_list_by_timestamp[timestamp]

        # observe the client spatial distribution at current timestamp
        total_clients = 0
        city_clients_by_name = {}
        for c in clients:
            total_clients += c['Count']
            if c['Count'] > 0:
                city_clients_by_name[c['City']] = c
        if total_clients == 0:
            continue

        # calculate ideal replica placement for XDN
        replica_group_info = {'Replicas': [], 'Leader': {'Name': ""}}
        if approach == 'XDN':
            replica_group_info = get_heuristic_replicas_placement(nf_edge_server_locations, 
                                                                  clients, 
                                                                  num_replicas, 
                                                                  is_silent=True)
        
        elif approach == 'CD':
            replica_group_info = {'Replicas': cd_replica_group, 
                                  'Leader': {'Name': static_nr_server}}
        
        elif approach == 'ED':
            # in Durable Object, there is no location reconfiguration after the first request
            # so we only calculate the palacement one time, when ed_replica_group is None.
            replica_group_info = ed_replica_group_info
            if ed_replica_group_info == None:
                ed_replica_group_info = get_heuristic_replicas_placement(nf_edge_server_locations, 
                                                                         clients, 
                                                                         1, 
                                                                         is_silent=True)
                replica_group_info = ed_replica_group_info
        
        elif approach == 'GD':
            replica_group_info = get_heuristic_replicas_placement(spanner_leader_locations, 
                                                                  clients, 
                                                                  1, 
                                                                  is_silent=True)
            placement_menu_name = spanner_placement_menu_per_leader[replica_group_info['Leader']['Name']]
            replica_group_member = spanner_placement_menu[placement_menu_name]
            replica_group = []
            for member in replica_group_member:
                replica_group.append(gcp_server_by_name[member])
            replica_group_info['Replicas'] = replica_group
        
        else:
            raise Exception(f'Unknown approach {approach}')
        
        # gather the required servers
        for r in replica_group_info['Replicas']:
            required_servers.add(r['Name'])
        
        
        # get the expected latency, grouped by city
        latencies_per_city = get_expected_latencies(replica_group_info['Replicas'], 
                                                    clients, 
                                                    replica_group_info['Leader']['Name'], 
                                                    c_lat_slowdown_factor,
                                                    is_report_per_city=True)
        
        # process the per city latencies
        print(f">>>> timestamp {timestamp}")
        print(f"      approach: {approach}")
        print(f"      replica group: {replica_group_info['Leader']['Name']}")
        print(f"      per-city avg. latencies:")
        latency_across_cities = []
        for city_name, latencies in latencies_per_city.items():
            if len(latencies) > 0:
                city_name_no_space = city_name.replace(" ", "")
                avg_lat_ms = statistics.mean(latencies)
                
                print(f"         city={city_name_no_space:15}\t avg_lat_ms={avg_lat_ms:5.2f}ms")
                active_cities.add(city_name_no_space)
                
                # collect the raw data
                assert city_name in city_clients_by_name, f'Unknown data for city {city_name}'
                city = city_clients_by_name[city_name]
                data = {'Timestamp': timestamp, 
                        'Approach': approach, 
                        'Latitude': city['Latitude'],
                        'Longitude': city['Longitude'],
                        'CityName': city_name,
                        'NumClient': city['Count'],
                        'AvgLatMilisecond': avg_lat_ms}
                raw_result.append(data)

            latency_across_cities.extend(latencies)
        print(f"      across city avg. latencies:  {statistics.mean(latency_across_cities):4.2f}ms")
        pass

    print()
    print()
    print(f">>> required servers [{len(required_servers)}]: {required_servers}")
    print(f">>> active cities    [{len(active_cities)}]: {active_cities}")

# ==============================================================================


# ==============================================================================
#                        Actual Measurement on Simulation
# ==============================================================================

# TODO: ACTUAL MEASUREMENT IN SIMULATED WAN
# - Prepare the cluster for XDN
#   - Prepare the config file
#   - Enable latency emulation
#   - Start the cluster
#   - Start latency injector for client

for approach in approaches:

    config_filename = f'temporal_xdn_{approach}.properties'

    for timestamp in timestamps:
        pass

# ==============================================================================


# ===== DATA PROCESSING ========================================================
# write the raw results into csv file, with the following format:
#   timestamp_utc, approach, city_lat, city_lon, city_name, client_count, avg_lat_ms
result_filename = 'temporal_latencies.csv'
with open(result_filename, 'w') as result_file:
    result_file.write('timestamp_utc, approach, city_lat, city_lon, city_name, client_count, avg_lat_ms\n')
    for data in raw_result:
        result_file.write(f"{data['Timestamp']},{data['Approach']},{data['Latitude']},{data['Longitude']},{data['CityName']},{data['NumClient']},{data['AvgLatMilisecond']}\n")
    result_file.close()

# sort city by latitude and longitude, from east to west.
# assign metro ID consistent across different approaches
# TODO: handle other approaches
sorted_cities = sorted(city_locations, key=lambda d: (d['Longitude'], d['Latitude']), reverse=True)
for i in range(len(sorted_cities)):
    city_name = sorted_cities[i]['City']
    sorted_cities[i]['MetroID'] = i + 1
sorted_city_names = []
for city in sorted_cities:
    sorted_city_names.append(city['City'])

latencies_xdn = []
latencies_gd = []
latencies_cd = []
latencies_ed = []
for approach in approaches:
    per_city_latencies = {}
    for data in raw_result:
        if data['Approach'] != approach:
            continue
        city_name = data['CityName']
        if city_name not in per_city_latencies:
            per_city_latencies[city_name] = []
        for i in range(data['NumClient']):
            per_city_latencies[city_name].append(data['AvgLatMilisecond'])
    
    per_city_avg_lat_ms = {}
    for city_name, latencies in per_city_latencies.items():
        if len(latencies) > 0:
            avg_lat_ms = statistics.mean(latencies)
            per_city_avg_lat_ms[city_name] = avg_lat_ms

    for i in range(len(sorted_cities)):
        city_name = sorted_cities[i]['City']
        curr_city_avg_lat_ms = 0
        if city_name in per_city_avg_lat_ms:
            curr_city_avg_lat_ms = per_city_avg_lat_ms[city_name]
        
        if approach == 'XDN':
            latencies_xdn.append(curr_city_avg_lat_ms)
        elif approach == 'CD':
            latencies_cd.append(curr_city_avg_lat_ms)
        elif approach == 'ED':
            latencies_ed.append(curr_city_avg_lat_ms)
        elif approach == 'GD':
            latencies_gd.append(curr_city_avg_lat_ms)
        else:
            raise Exception(f'Unsupported approach {approach}')


# actually plot the data
num_metros = len(sorted_cities)
x = 1 + np.arange(num_metros)

# Example data (replace with real latency data)
latencies_1 = latencies_gd
latencies_2 = latencies_cd
latencies_3 = latencies_ed
latencies_4 = latencies_xdn

# Create figure + 4×1 subplots
fig, axes = plt.subplots(4, 1, figsize=(4.5, 5))

# List of data arrays for convenience
all_latencies = [latencies_1, latencies_2, latencies_3, latencies_4]
titles = ['Multi Region', 'Static', 'Durable Object', 'XDN']
colors = ['red', 'green', 'orange', 'gold']

# Flatten axes so we can iterate easily
axes_flat = axes.flat
idxs = np.arange(4)

for idx, ax, latencies, title, color in zip(idxs, axes_flat, all_latencies, titles, colors):
    # Plot the bar chart
    ax.bar(x, latencies, color=color, alpha=0.4)

    if idx == 3:
        updated_sorted_city_names = []
        for i, name in enumerate(sorted_city_names):
            if i % 2 == 0:
                updated_sorted_city_names.append(name)
            else:
                updated_sorted_city_names.append("")
        ax.set_xticks(x, sorted_city_names, rotation=90, fontsize=7)
        ax.set_xlabel('Metro Area')
    else:
        ax.set_xticks([])
    
    # Remove left/right padding
    ax.set_xlim(left=0.5, right=50.5)
    ax.set_ylim(bottom=0, top=90)
    
    # Titles and labels
    ax.text(1, 65, title, fontsize=12)
    ax.set_ylabel(' ')
    
    # Compute the average
    mean_val = np.mean(latencies)
    
    # Add a horizontal line at the average
    ax.axhline(y=mean_val, color='red', linestyle='--', linewidth=1)
    
    # Option A: Place the text in data coordinates near the horizontal line
    # Adjust 'va' and 'ha' to avoid overlapping the line if you prefer
    ax.text(
        x[-1],              # Place text near the right-most bar
        mean_val,           # Align with the average line vertically
        f"{mean_val:.2f} ms",
        ha='right',
        va='bottom',
        color='black',
        fontsize=10,
        weight="bold"
    )

fig.text(0.04, 0.6, 'Avg. Latency (ms)', va='center', rotation='vertical')

# If text or axes are clipped, you can do:
plt.tight_layout()
plt.savefig("latency_temporal_clients.pdf", format="pdf", bbox_inches="tight")
plt.show()

exit(0)