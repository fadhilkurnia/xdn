# read clients
# split clients into 4 timezone
# create client distribution for every 30 minutes following the sinusoidal distribution

import os
import re
import copy
import time
import json
import statistics
import subprocess
import numpy as np
import matplotlib.pyplot as plt
from replica_group_picker import get_client_locations
from replica_group_picker import get_heuristic_replicas_placement
from replica_group_picker import find_k_closest_servers
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

population_data_file = "location_distributions/client_world_metro_population.csv"
server_edge_location_file = "location_distributions/server_netflix_oca.csv"
server_aws_region_location_file = "location_distributions/server_aws_region.csv"
server_gcp_region_location_file = "location_distributions/server_gcp_region.csv"
gigapaxos_template_config_file = "../conf/gigapaxos.xdnlat.template.properties"
request_payload_file="measurement_payload.json"

approaches = ['XDN', 'CD', 'ED', 'GD']
num_cloudlab_machines = 12
control_plane_address="10.10.1.11"
net_device_if_name="enp3s0f0np0"        # example: "ens1f1np1"
net_device_if_name_exception_list=""    # example: "10.10.1.6/enp130s0f0np0,10.10.1.8/enp4s0f0np0"
control_plane_http_port="3300"
max_num_clients = 1000
num_replicas = 3
static_nr_server = 'us-east-1'
c_lat_slowdown_factor = 0.32258064516
request_endpoint="/api/books"
is_cache_docker_image=True
is_run_real_measurement=False
is_print_client_temporal_dist=False
enable_inter_city_lat_emulation=True

city_locations = get_client_locations(population_data_file)
aws_region_servers = get_server_locations([server_aws_region_location_file])
gcp_region_servers = get_server_locations([server_gcp_region_location_file])
nf_edge_server_locations = get_server_locations([server_edge_location_file], 
                                                remove_duplicate_location=False,
                                                remove_nonredundant_city_servers=True)

# replace dash with underscore as gigapaxos does not support dash in the server name
for i, server in enumerate(nf_edge_server_locations):
    nf_edge_server_locations[i]["Name"] = nf_edge_server_locations[i]["Name"].replace("-", "_")
for i, server in enumerate(aws_region_servers):
    aws_region_servers[i]["Name"] = aws_region_servers[i]["Name"].replace("-", "_")
gcp_og_server_locations = copy.deepcopy(gcp_region_servers)
for i, server in enumerate(gcp_region_servers):
    gcp_region_servers[i]["Name"] = gcp_region_servers[i]["Name"].replace("-", "_")
static_nr_server = static_nr_server.replace("-", "_")

# prepare static location for CD
cd_replica_group = []
for s in aws_region_servers:
    if s['Name'] == static_nr_server:
        cd_replica_group.append(s)
        break

# provide replica group options for multi-region datastore (e.g., spanner)
# deduplicates menu in spanner with same region name
spanner_placement_menu_info = get_spanner_placement_menu(gcp_og_server_locations)
spanner_placement_menu = spanner_placement_menu_info["PlacementMenu"]
spanner_placement_menu_per_leader = spanner_placement_menu_info["PlacementMenuByLeader"]
gcp_og_server_by_name = {}
for server in gcp_og_server_locations:
    gcp_og_server_by_name[server["Name"]] = server
spanner_replica_group_by_menu = {}
for menu, placement in spanner_placement_menu.items():
    replicas = []
    for server_name in placement:
        replicas.append(gcp_og_server_by_name[server_name].copy())
    for i, server in enumerate(replicas):
        replicas[i]["Name"] = server["Name"].replace("-", "_")
    replicas[0]["Name"] = replicas[0]["Name"] + "_a"
    replicas[1]["Name"] = replicas[1]["Name"] + "_b"
    spanner_replica_group_by_menu[menu] = {"Replicas": replicas, "Leader": replicas[0].copy()}
spanner_leader_locations = []
for leader_name, menu in spanner_placement_menu_per_leader.items():
    server = gcp_og_server_by_name[leader_name]
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

xdn_required_servers = []
xdnnr_required_servers = []
cd_required_servers = []
ed_required_servers = []
gd_required_servers = []

replica_group_info_per_approach_tz = {}

required_server_by_name = {}

# do numerical latency measurement for each approach
raw_result = []
for approach in approaches:
    tz_latencies_per_city = {}
    required_servers = set()        # capture the required servers to be emulated
    active_cities = set()           # capture all cities with clients

    ed_replica_group_info = None
    gd_replica_group_info = None

    replica_group_info_per_approach_tz[approach] = {}
    
    for timestamp in timestamps:
        replica_group_info_per_approach_tz[approach][timestamp] = None
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
        
        elif approach == 'XDNNR':
            replica_group_info = get_heuristic_replicas_placement(nf_edge_server_locations, 
                                                                  clients, 
                                                                  1, 
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
            # in GD/MultiRegion/Spanner we assume no automatic reconfiguration, thus we keep
            # the first replica group placement info.
            replica_group_info = gd_replica_group_info
            if gd_replica_group_info == None:
                gd_replica_group_info = get_heuristic_replicas_placement(spanner_leader_locations, 
                                                                         clients, 
                                                                         1, 
                                                                         is_silent=True)
                placement_menu_name = spanner_placement_menu_per_leader[gd_replica_group_info['Leader']['Name']]
                gd_replica_group_info = copy.deepcopy(spanner_replica_group_by_menu[placement_menu_name])
                replica_group_info = gd_replica_group_info
        
        else:
            raise Exception(f'Unknown approach {approach}')
        
        # gather the required servers
        replica_group_info_per_approach_tz[approach][timestamp] = copy.deepcopy(replica_group_info)
        for r in replica_group_info['Replicas']:
            required_servers.add(r['Name'])
            required_server_by_name[r['Name']] = r
        
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
                med_latency = statistics.median(latencies)
                min_latency = min(latencies)
                max_latency = max(latencies)
                p90_latency = np.percentile(latencies, 90)
                p95_latency = np.percentile(latencies, 95)
                p99_latency = np.percentile(latencies, 99)
                
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
                        'AvgLatMilisecond': avg_lat_ms,
                        'MinLatMilisecond': min_latency,
                        'MaxLatMilisecond': max_latency,
                        'P50LatMilisecond': med_latency,
                        'P90LatMilisecond': p90_latency,
                        'P95LatMilisecond': p95_latency,
                        'P99LatMilisecond': p99_latency,
                        }
                raw_result.append(data)

            latency_across_cities.extend(latencies)
        print(f"      across city avg. latencies:  {statistics.mean(latency_across_cities):4.2f}ms")
        pass

    print()
    print()
    print(f">>> required servers [{len(required_servers)}]: {required_servers}")
    print(f">>> active cities    [{len(active_cities)}]: {active_cities}")
    
    if approach == "XDN":
        xdn_required_servers = required_servers
    elif approach == "XDNNR":
        xdnnr_required_servers = required_servers
    elif approach == "GD":
        gd_required_servers = required_servers
    elif approach == "ED":
        ed_required_servers = required_servers
    elif approach == "CD":
        cd_required_servers = required_servers
    else:
        raise Exception(f'Unknown approach {approach}')

# write the raw results into a csv file, with the following format:
#   timestamp_utc, approach, city_lat, city_lon, city_name, client_count, avg_lat_ms, min_lat_ms, max_lat_ms, p50_lat_ms, p90_lat_ms, p95_lat_ms, p99_lat_ms
result_filename = 'temporal_latencies_expected.csv'
with open(result_filename, 'w') as result_file:
    result_file.write('timestamp_utc,approach,city_lat,city_lon,city_name,client_count,avg_lat_ms,min_lat_ms,max_lat_ms,p50_lat_ms,p90_lat_ms,p95_lat_ms,p99_lat_ms\n')
    for data in raw_result:
        result_file.write(f"{data['Timestamp']},{data['Approach']},{data['Latitude']},{data['Longitude']},{data['CityName']},{data['NumClient']},{data['AvgLatMilisecond']},{data['MinLatMilisecond']},{data['MaxLatMilisecond']},{data['P50LatMilisecond']},{data['P90LatMilisecond']},{data['P95LatMilisecond']},{data['P99LatMilisecond']}\n")
    result_file.close()

# ==============================================================================


# ==============================================================================
#                        Actual Measurement on Simulation
# ==============================================================================

# ACTUAL MEASUREMENT IN SIMULATED WAN
# - Prepare the cluster for XDN
#   - Prepare the config file
#   - Enable latency emulation
#   - Start the cluster
#   - Start latency injector for client

if is_run_real_measurement:

    print()
    print()
    print()
    print()

    # ensure we have enough machines for XDN
    assert len(xdn_required_servers) <= num_cloudlab_machines, f"Not enough machines available to emulate XDN"

    # verifying the required programs
    command = "ls xdn_latency_proxy_go/latency-injector"
    ret_code = os.system(command)
    assert ret_code == 0, f"cannot find the xdn_latency_proxy program"
    command = "xdn --help > /dev/null"
    ret_code = os.system(command)
    assert ret_code == 0, f"cannot find the xdn program"
    command = "mkdir -p results_lat_temporal"
    ret_code = os.system(command)
    assert ret_code == 0, f"cannot prepare directory for latency results"

    # pull the used docker images in all machines, which is helpful to mitigate
    # the pulls limit from Docker Hub.
    if is_cache_docker_image:
        for i in range(num_cloudlab_machines):
            command = f'ssh 10.10.1.{i+1} "docker pull fadhilkurnia/xdn-bookcatalog:latest"'
            res = subprocess.run(command, shell=True, capture_output=True, text=True)
            assert res.returncode == 0, f"ERROR: {res.stdout}"
        command = f'ssh {control_plane_address} "docker pull fadhilkurnia/xdn-bookcatalog:latest"'
        res = subprocess.run(command, shell=True, capture_output=True, text=True)
        assert res.returncode == 0, f"ERROR: out={res.stdout} err={res.stderr}"

    for approach in approaches:

        print(f"\n\n>> ======================================================================")
        print(f">> Start measurement for approach={approach}")
        print(f">> ======================================================================")

        # pre-process the used servers
        config_filename = f'temporal_{approach}.properties'
        required_server_names = []
        if approach == "XDN":
            required_server_names = xdn_required_servers
        elif approach == "XDNNR":
            required_server_names = xdnnr_required_servers
        elif approach == "GD":
            required_server_names = gd_required_servers
        elif approach == "CD":
            required_server_names = cd_required_servers
        elif approach == "ED":
            required_server_names = ed_required_servers
        else:
            raise Exception(f'Unknown approach {approach}')
        
        # prepare config string, including assigning address for each server name
        machine_counter = 1
        server_name_address=""
        server_name_geolocation=""
        server_address_by_name = {}
        for server_name in required_server_names:
            assert server_name in required_server_by_name
            server = required_server_by_name[server_name]
            escaped_server_name = server_name.replace("-", "_") # convert '-' into '_' as gigapaxos does not support '-'
            server_name_address += f"active.{escaped_server_name}=10.10.1.{machine_counter}:2000\n"
            server_name_geolocation += f"active.{escaped_server_name}.geolocation=\"{server['Latitude']},{server['Longitude']}\"\n"
            server_address_by_name[escaped_server_name] = f"10.10.1.{machine_counter}"
            machine_counter += 1
        assert machine_counter <= num_cloudlab_machines + 1

        # replace gigapaxos config file
        print(" > updating gigapaxos config file")
        template_cfg_content = None
        with open(gigapaxos_template_config_file, 'r') as file:
            template_cfg_content = file.read()
        udpated_cfg_content = re.sub(
            "___________PLACEHOLDER_ACTIVES_ADDRESS___________", 
            server_name_address, 
            template_cfg_content)
        udpated_cfg_content = re.sub(
            "___________PLACEHOLDER_ACTIVES_GEOLOCATION___________", 
            server_name_geolocation, 
            udpated_cfg_content)
        udpated_cfg_content = re.sub(
            "___________PLACEHOLDER_RC_HOST___________", 
            control_plane_address, 
            udpated_cfg_content)
        updated_cfg_file = f"../conf/{config_filename}"
        with open(updated_cfg_file, 'w') as file:
            file.write(udpated_cfg_content)

        # emulate and verify inter-server latency
        if enable_inter_city_lat_emulation:
            max_attempt = 3; curr_attempt = 0; is_success = False
            while curr_attempt < max_attempt and not is_success:
                try:
                    print(f" > emulating edge inter-server latencies, net_dev_if={net_device_if_name}, exception={net_device_if_name_exception_list}")
                    net_dev_exception_flag=""
                    if net_device_if_name_exception_list != "":
                        net_dev_exception_flag = f"--device-exceptions={net_device_if_name_exception_list}"
                    command = f"python ../bin/xdnd-emulate-latency.py --device={net_device_if_name} --config={updated_cfg_file} {net_dev_exception_flag}"
                    print("   ", command)
                    ret_code = os.system(command)
                    assert ret_code == 0
                    command = f"python ../bin/xdnd-emulate-latency.py --mode=verify --config={updated_cfg_file}"
                    print("   ", command)
                    ret_code = os.system(command)
                    assert ret_code == 0
                    is_success = True
                except:
                    time.sleep(10 ** (curr_attempt))
                    curr_attempt += 1
                finally:
                    assert curr_attempt < max_attempt, f'Failed to emulate and verify latency after {max_attempt} attempts'

        # clear any remaining running xdn or other processes
        print(f" > resetting the measurement cluster:")
        for i in range(num_cloudlab_machines):
            os.system(f"ssh 10.10.1.{i+1} sudo fuser -k 2000/tcp")
            os.system(f"ssh 10.10.1.{i+1} sudo fuser -k 2300/tcp")
            os.system(f"ssh 10.10.1.{i+1} sudo rm -rf /tmp/gigapaxos")
            os.system(f"ssh 10.10.1.{i+1} docker network prune --force > /dev/null 2>&1")
            os.system(f"ssh 10.10.1.{i+1} sudo fuser -k 4001/tcp")
            os.system(f"ssh 10.10.1.{i+1} sudo rm -rf /tmp/rqlite_data")
            os.system(f"ssh 10.10.1.{i+1} 'containers=$(docker ps -a -q); if [ -n \"$containers\" ]; then docker stop $containers; fi'")
            ret_code = os.system(f'ssh 10.10.1.{i+1} "rm -rf xdn/eval/durable_objects/bookcatalog/.wrangler/state/"')
            assert ret_code == 0
        os.system(f"ssh {control_plane_address} sudo fuser -k 3000/tcp")
        os.system(f"ssh {control_plane_address} sudo rm -rf /tmp/gigapaxos")

        # start latency-injector proxy in the background in this driver machine
        screen_session_base_name=f"temporal_scr_{approach}"
        proxy_screen_name = screen_session_base_name + "_prx"
        print(f" > starting client's latency injector, location_config={updated_cfg_file}")
        command = f"fuser -s -k 8080/tcp"
        print("   ", command)
        os.system(command)
        command = f"screen -S {proxy_screen_name} -X quit > /dev/null 2>&1"
        print("   ", command)
        os.system(command)
        os.system(f"rm -f screen_logs/{proxy_screen_name}.log")
        command = f"RUST_LOG=debug screen -L -Logfile screen_logs/{proxy_screen_name}.log -S {proxy_screen_name} -d -m ./xdn_latency_proxy_go/latency-injector -config={updated_cfg_file}"
        print("   ", command)
        ret_code = os.system(command)
        assert ret_code == 0

        # deploys xdn in the prepared machines
        print(f" > starting the measurement cluster:")
        for server_name, address in server_address_by_name.items():
            print(f"   + {server_name}: {address}")
        if approach == "XDN" or approach == "CD" or approach == "XDNNR":
            gp_screen_name = screen_session_base_name + "_gp"
            command = f"screen -S {gp_screen_name} -X quit  > /dev/null 2>&1"
            print("   ", command)
            os.system(command)
            os.system(f"rm -f screen_logs/{gp_screen_name}.log")
            command = f"screen -L -Logfile screen_logs/{gp_screen_name}.log -S {gp_screen_name} -d -m bash -c '../bin/gpServer.sh -DgigapaxosConfig={updated_cfg_file} start all; exec bash'"
            print("   ", command)
            ret_code = os.system(command)
            assert ret_code == 0
            time.sleep(120)
        elif approach == "GD" or approach == "ED":
            # do nothing as we will start rqlite or durable object later
            pass

        # deploy the service
        print(f"   > deploying the service")
        deployed_service_name=f"bookcatalog"
        if approach == "XDN" or approach == "CD" or approach == "XDNNR":
            command = f"XDN_CONTROL_PLANE={control_plane_address} xdn launch {deployed_service_name} --image=fadhilkurnia/xdn-bookcatalog --deterministic=true --consistency=linearizability --state=/app/data/"
            print("   ", command)
            res = subprocess.run(command, shell=True, capture_output=True, text=True)
            assert res.returncode == 0
            output = res.stdout
            assert "Error" not in output, f"output: {output}"
            time.sleep(60)
        elif approach == "GD" or approach == "ED":
            # do nothing as we will start them on reconfiguration later
            pass

        prev_ts_leader = None
        
        for timestamp in timestamps:
            clients = client_list_by_timestamp[timestamp]

            # skip this timestamp if there is no clients
            total_clients = 0
            for c in clients:
                total_clients += c['Count']
            if total_clients == 0:
                continue

            # reconfigure replica group
            replica_group_info = replica_group_info_per_approach_tz[approach][timestamp]
            assert replica_group_info is not None, f"Unknown replica group info for approach {approach} at ts={timestamp}"
            leader_name = replica_group_info['Leader']['Name']
            leader_address = server_address_by_name[leader_name]
            if approach == "XDN" or approach == "XDNNR" or approach == "CD":
                #  only reconfigure if the replica group/leader changes
                if prev_ts_leader == None or prev_ts_leader != replica_group_info['Leader']['Name']:
                    prev_ts_leader = leader_name
                    replica_names = []
                    for replica in replica_group_info["Replicas"]:
                        replica_names.append(replica['Name'])
                    command = f'curl -X POST http://{control_plane_address}:{control_plane_http_port}/api/v2/services/{deployed_service_name}/placement -d "{{"NODES" : {replica_names}, "COORDINATOR": "{leader_name}"}}"'
                    print("   ", command)
                    res = subprocess.run(command, shell=True, capture_output=True, text=True)
                    assert res.returncode == 0
                    output = res.stdout
                    print("   ", output)
                    output_json = json.loads(output)
                    if "FAILED" in output_json:
                        assert output_json["FAILED"] == False
                    time.sleep(90)
            elif approach == "GD":
                assert len(replica_group_info["Replicas"]) == 3
                #  only reconfigure if the replica group/leader changes
                if prev_ts_leader == None or prev_ts_leader != replica_group_info['Leader']['Name']:
                    prev_ts_leader = leader_name
                    for server_id, server in enumerate(replica_group_info["Replicas"]):
                        node_id = server_id + 1
                        address = server_address_by_name[server["Name"]]
                        command = f'ssh {leader_address} nohup rqlited -node-id={node_id} -http-addr={leader_address}:4001 -raft-addr={leader_address}:4002 /tmp/rqlite_data > /dev/null 2>&1 &'
                        if server["Name"] != leader_name:
                            command = f'ssh {address} nohup rqlited -node-id={node_id} -http-addr={address}:4001 -raft-addr={address}:4002 -join={leader_address}:4002 /tmp/rqlite_data > /dev/null 2>&1 &'
                        print("   ", command)
                        ret_code = os.system(command)
                        assert ret_code == 0
                        time.sleep(5)
                    for server_id, server in enumerate(replica_group_info["Replicas"]):
                        node_id = server_id + 1
                        address = server_address_by_name[server["Name"]]
                        command = f'ssh {address} docker run --rm --detach --env DB_TYPE=rqlite --env DB_HOST={address} --publish 2300:80 --name bookcatalog_gd_{node_id} fadhilkurnia/xdn-bookcatalog > /dev/null 2>&1'
                        print("   ", command)
                        ret_code = os.system(command)
                        assert ret_code == 0
                    time.sleep(5)
                pass
            elif approach == "ED":
                assert len(replica_group_info["Replicas"]) == 1
                #  only reconfigure if the replica group/leader changes
                if prev_ts_leader == None or prev_ts_leader != replica_group_info['Leader']['Name']:
                    prev_ts_leader = leader_name
                    replica = replica_group_info["Leader"]
                    address = server_address_by_name[replica["Name"]]
                    log_filename = f'{screen_session_base_name}_t{timestamp}.log'
                    command = f'ssh {address} "cd xdn/eval/durable_objects/bookcatalog && npm install > /dev/null 2>&1 && fuser -k 2300/tcp"'
                    ret_code = os.system(command)
                    command = f'ssh {address} "cd xdn/eval/durable_objects/bookcatalog && nohup npx wrangler dev --ip {address} --port 2300 > {log_filename}" &'
                    print("   ", command)
                    ret_code = os.system(command)
                    assert ret_code == 0
                    time.sleep(5)
                pass

            # get the closest replica for each client
            target_replica_address_by_city_client_name = {}
            for client in clients:
                client_city_name = client['City'].replace(" ", "")
                client_lat = float(client["Latitude"])
                client_lon = float(client["Longitude"])
                closest_servers = find_k_closest_servers(replica_group_info["Replicas"], {"Latitude": client_lat, "Longitude": client_lon}, 1)
                assert len(closest_servers) == 1
                closest_server_name = closest_servers[0]['Name']
                assert closest_server_name in server_address_by_name, f"Unknown address for server {closest_server_name}"
                target_address = server_address_by_name[closest_server_name]
                target_replica_address_by_city_client_name[client_city_name] = target_address

            # start small warmup
            command = f"ab -X 127.0.0.1:8080 -k -p {request_payload_file} -T application/json -H 'XDN: {deployed_service_name}' -c 3 -n 30 http://{leader_address}:2300{request_endpoint} 2>/dev/null | grep \"Time per request:\""
            print("\n   ", command)
            max_repetitions = 3; num_attempt = 0; ret_code = 0
            while num_attempt < max_repetitions:
                ret_code = os.system(command)
                if ret_code == 0:
                    break
                num_attempt += 1
                if num_attempt < max_repetitions:
                    time.sleep(10 ** (num_attempt))
            assert ret_code == 0, f"Warmup failed after {max_repetitions} attempts."

            print(f">> running measurement of approach={approach} at time={timestamp}")
            for client in clients:
                # TODO run all clients in parallel
                num_city_clients = client['Count']
                city_name = client['City'].replace(" ", "")
                if num_city_clients == 0:
                    continue
                num_requests = 10 * client['Count'] # 10 requests per client
                client_lat = client['Latitude']
                client_lon = client['Longitude']
                target_address = target_replica_address_by_city_client_name[city_name]
                target_latency_file = f"results_lat_temporal/lat_temporal_{approach}_t{timestamp}_c{city_name}.tsv"
                command = f"ab -X 127.0.0.1:8080 -k -p {request_payload_file} -g {target_latency_file} -T application/json -H 'XDN: {deployed_service_name}' -c 1 -n {num_requests} -H 'X-Client-Location: {client_lat};{client_lon}' http://{target_address}:2300{request_endpoint} 2>/dev/null | grep \"Time per request:\""
                print(f"  - {city_name:15}: {command}")
                max_repetitions = 3; num_attempt = 0; ret_code = 0
                while num_attempt < max_repetitions:
                    ret_code = os.system(command)
                    if ret_code == 0:
                        break
                    num_attempt += 1
                    if num_attempt < max_repetitions:
                        time.sleep(10 ** (num_attempt))
                assert ret_code == 0, f"Command failed after {max_repetitions} attempts."

            pass

        # destroy the service after we iterate through all the timestamps
        print(f" > removing the deployed service")
        if approach == "XDN" or approach == "CD" or approach == "XDNNR":
            command = f"yes \"yes\" | XDN_CONTROL_PLANE={control_plane_address} xdn service destroy {deployed_service_name}"
            print("   ", command)
            ret_code = os.system(command)
            assert ret_code == 0 or ret_code == 100 
            # ret_code 100 is for timeout, it is fine to ignore the timeout error (fail open) since we 
            # decouple the service name for different locality, also we will destroy the service anyway 
            # at the end of the batch.
        elif approach == "GD":
            for server_id, server in enumerate(replica_group_info["Replicas"]):
                node_id = server_id + 1
                address = server_address_by_name[server["Name"]]
                command = f'ssh {address} docker stop bookcatalog_gd_{node_id}'
                print("   ", command)
                ret_code = os.system(command)
                assert ret_code == 0
                ret_code = os.system(f"ssh {address} sudo fuser -k 4001/tcp")
                assert ret_code == 0
                ret_code = os.system(f"ssh {address} sudo rm -rf /tmp/rqlite_data")
                assert ret_code == 0
            pass
        elif approach == "ED":
            for server_id, server in enumerate(replica_group_info["Replicas"]):
                address = server_address_by_name[server["Name"]]
                command = f'ssh {address} fuser -k 2300/tcp'
                print("   ", command)
                ret_code = os.system(command)
                assert ret_code == 0
                command = f'ssh {address} rm -rf xdn/eval/durable_objects/bookcatalog/.wrangler/state/'
                print("   ", command)
                ret_code = os.system(command)
                assert ret_code == 0
                log_filename = f'{screen_session_base_name}_t*.log'
                command = f'scp -q -o LogLevel=QUIET "{address}:~/xdn/eval/durable_objects/bookcatalog/{log_filename}" screen_logs && ssh {address} "rm -rf \'xdn/eval/durable_objects/bookcatalog/{log_filename}\'"'
                print("   ", command)
                ret_code = os.system(command)
                assert ret_code == 0
        else:
            raise Exception(f"Unknown approach {approach}")

        # destroy the cluster
        print(f" > removing the xdn cluster")
        command = f"../bin/gpServer.sh -DgigapaxosConfig={updated_cfg_file} forceclear all > /dev/null 2>&1"
        print("   ", command)
        ret_code = os.system(command)
        assert ret_code == 0

        time.sleep(60)


# ==============================================================================