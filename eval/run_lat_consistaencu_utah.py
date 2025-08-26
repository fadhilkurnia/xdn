import os
import time
import json
import random
import requests
import subprocess
import statistics
import numpy as np
import pandas as pd
from utils import cluster_locations
from utils import replace_placeholder
from utils import get_server_locations
from utils import get_per_city_clients
from utils import get_expected_latencies
from utils import get_client_count_per_city
from utils import get_population_ratio_per_city
from replica_group_picker import get_client_locations
from replica_group_picker import find_k_closest_servers
from replica_group_picker import get_heuristic_replicas_placement
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

# ==============================================================================

def get_protocol_aware_replica_placement(protocol_class, client_distribution, server_distribution, read_ratio):
    assert protocol_class == "linearizability" or protocol_class == "sequential" or protocol_class == "eventual", f"Unknown protocol class"
    assert len(client_distribution) > 0
    assert len(server_distribution) > 0

    server_by_name = {}
    for server in server_distribution:
        server_by_name[server['Name']] = server

    if protocol_class == "linearizability":
        num_replicas = 3
        return get_heuristic_replicas_placement(server_distribution, client_distribution, num_replicas, True)
    
    if protocol_class == "sequential":
        import math
        num_replicas = math.ceil(3 + (10-3) * float(read_ratio / 100.0))
        num_rw_replicas = (num_replicas + 1) // 2
        num_ro_replicas = num_replicas - num_rw_replicas
        if num_replicas == 3:
            num_rw_replicas = 3
            num_ro_replicas = 0
        rw_replica_group_info = get_heuristic_replicas_placement(server_distribution, 
                                                                 client_distribution, 
                                                                 num_rw_replicas, 
                                                                 True)
        rw_replica_name_set = set()
        for server in rw_replica_group_info['Replicas']:
            rw_replica_name_set.add(server['Name'])
        
        # decide the read-only replica locations
        # TODO: non repetitive replicas
        read_replica_name_set = set()
        if num_ro_replicas > 0:
            location_list = []
            for client in client_distribution:
                for i in range(client['Count']):
                    location_list.append((client['Latitude'], client['Longitude']))
            _, centroids = cluster_locations(location_list, num_ro_replicas)
            read_replicas = []
            for center in centroids:
                ref_server = {'Name': 'ReferenceServer', 'Latitude': center[0], 'Longitude': center[1]}
                closest_servers = find_k_closest_servers(server_distribution, ref_server, 1)
                assert len(closest_servers) == 1
                read_replica_name_set.add(closest_servers[0]['Name'])
                read_replicas.append(closest_servers[0])
            assert len(read_replica_name_set) == num_ro_replicas, f"We need more read only server in this case"
        
            # assign each client to the closest read-only replicas
            dummy_clients = []
            read_replica_demand_aggr = {}
            for client in client_distribution:
                closest_servers = find_k_closest_servers(read_replicas, client, 1)
                assert len(closest_servers) == 1
                closest_server = closest_servers[0]
                if closest_server['Name'] not in read_replica_demand_aggr:
                    read_replica_demand_aggr[closest_server['Name']] = 0
                read_replica_demand_aggr[closest_server['Name']] += client['Count']
            for read_server in read_replicas:
                read_server['Count'] = read_replica_demand_aggr[read_server['Name']]
                dummy_clients.append(read_server) 
        
        replica_group_info = rw_replica_group_info
        for server_name in read_replica_name_set:
            assert server_name not in rw_replica_name_set, f"Redundant rw and ro replicas of {server_name}: {rw_replica_name_set} vs. {read_replica_name_set}"
            assert server_name in server_by_name, f"Unknown server data for {server_name}"
            curr_server = server_by_name[server_name]
            replica_group_info['Replicas'].append(curr_server)
        print(f'protocol class: {protocol_class} rw:{len(rw_replica_name_set)} ro:{len(read_replica_name_set)}')
        return replica_group_info

    if protocol_class == "eventual":
        num_replicas = 10
        location_list = []
        for client in client_distribution:
            for i in range(client['Count']):
                location_list.append((client['Latitude'], client['Longitude']))
        _, centroids = cluster_locations(location_list, num_replicas)
        replica_name_set = set()
        for center in centroids:
            ref_server = {'Name': 'ReferenceServer', 'Latitude': center[0], 'Longitude': center[1]}
            closest_servers = find_k_closest_servers(server_distribution, ref_server, 1)
            replica_name_set.add(closest_servers[0]['Name'])
        replica_group_info = {'Replicas': [], 'Leader': None, 'Centroid': None}
        for server_name in replica_name_set:
            assert server_name in server_by_name, f"Unknown server data for {server_name}"
            curr_server = server_by_name[server_name]
            replica_group_info['Replicas'].append(curr_server)
        return replica_group_info

    raise Exception("Invalid input provided")

def prepare_xdn_config_file(config_file_template, config_target_filename, 
                            replica_group_info, control_plane_host):
    """
    Returns the mapping between service name and replica IP address, assuming
    the typical cloudlab local IP addresses of 10.10.1.*.
    """

    # prepare the config file
    command = f"cp {config_file_template} {config_target_filename}"
    ret_code = os.system(command=command)
    assert ret_code == 0

    # prepare text to be written into the target config file
    # while also creating mapping of server name to address
    num_servers = len(replica_group_info['Replicas'])
    assert len(replica_group_info['Replicas']) > 0, f"Expecting non-empty set of servers"
    server_address_by_name = {}
    curr_counter = 1
    server_name_address_txt = ""
    server_name_geolocation_txt = ""
    for server in replica_group_info['Replicas']:
        curr_server_name = server['Name']
        curr_address = f"10.10.1.{curr_counter}"
        server_address_by_name[curr_server_name] = curr_address
        server_name_address_txt += f"active.{curr_server_name}={curr_address}:2000\n"
        server_name_geolocation_txt += f"active.{curr_server_name}.geolocation=\"{server['Latitude']},{server['Longitude']}\"\n"
        curr_counter += 1
        pass

    # fill out the template in the config file, including mapping the address
    replace_placeholder(config_target_filename, "___________PLACEHOLDER_ACTIVES_ADDRESS___________", server_name_address_txt)
    replace_placeholder(config_target_filename, "___________PLACEHOLDER_ACTIVES_GEOLOCATION___________", server_name_geolocation_txt)
    replace_placeholder(config_target_filename, "___________PLACEHOLDER_RC_HOST___________", control_plane_host)


    return server_address_by_name

def generate_replica_dist_map_figure(target_figure_filename, replica_group_info, clients=None):
    plt.clf()
    plt.figure(figsize=(3, 3))
    
    # Define map boundaries for North America
    lat_min, lat_max = 24, 48
    lon_min, lon_max = -126, -72

    # Set up the map using Basemap (in 'merc' projection)
    m = Basemap(projection='merc',
                llcrnrlat=lat_min, urcrnrlat=lat_max,
                llcrnrlon=lon_min, urcrnrlon=lon_max,
                resolution='i')  # 'i' for intermediate detail
    
    # Draw map features
    m.fillcontinents(color='lightgray', lake_color='white')
    m.drawmapboundary(fill_color='white')

    # Convert lat/lon to x/y on the map
    data_replica = []
    for server in replica_group_info['Replicas']:
        data_replica.append([server['Latitude'], server['Longitude']])
    df_rep = pd.DataFrame(data_replica, columns=['Latitude', 'Longitude'])
    x_rep, y_rep = m(df_rep['Longitude'].values, df_rep['Latitude'].values)
    data_leader = []
    if replica_group_info['Leader'] != None:
        data_leader.append([replica_group_info['Leader']['Latitude'], replica_group_info['Leader']['Longitude']])
        df_leader = pd.DataFrame(data_leader, columns=['Latitude', 'Longitude'])
        x_leader, y_leader = m(df_leader['Longitude'].values, df_leader['Latitude'].values)
    data_clients = []
    if clients != None:
        for c in clients:
            data_clients.append([c['Latitude'], c['Longitude']])
        df_clients = pd.DataFrame(data_clients, columns=['Latitude', 'Longitude'])
        x_clients, y_clients = m(df_clients['Longitude'].values, df_clients['Latitude'].values)

    # Plot replica locations
    m.scatter(x_rep, y_rep, marker='o', color='orange', zorder=5, s=70, edgecolor='black', linewidth=1.0)
    if replica_group_info['Leader'] != None:
        m.scatter(x_leader, y_leader, marker='o', color='red', zorder=5, s=70, edgecolor='black', linewidth=1.0)
    if clients != None:
        m.scatter(x_clients, y_clients, marker='o', color='gray', zorder=5, s=10, edgecolor='black', linewidth=1.0)

    plt.savefig(target_figure_filename, bbox_inches='tight')
    return

def reset_xdn_cloudlab_cluster(num_machines, control_plane_address):
    print(f" > resetting the measurement cluster ...")
    for i in range(num_machines):
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
    return

def start_xdn_cloudlab_cluster(screen_session_name, gigapaxos_config_filename):
    screen_log_filename = f"screen_logs/{screen_session_name}.log"
    os.system(f"rm -rf {screen_log_filename}")
    command = f"screen -S {screen_session_name} -X quit > /dev/null 2>&1"
    print("   ", command)
    os.system(command)
    command = f"screen -L -Logfile {screen_log_filename} -S {screen_session_name} -d -m bash -c '../bin/gpServer.sh -DgigapaxosConfig={gigapaxos_config_filename} start all; exec bash'"
    print("   ", command)
    ret_code = os.system(command)
    assert ret_code == 0
    return

def emulate_inter_server_latency(gigapaxos_config_filename, net_device_if_name, 
                                 net_device_if_name_exception_list):
    max_attempt = 3; curr_attempt = 0; is_success = False
    while curr_attempt < max_attempt and not is_success:
        try:
            print(f" > emulating edge inter-server latencies, net_dev_if={net_device_if_name}, exception={net_device_if_name_exception_list}")
            net_dev_exception_flag=""
            if net_device_if_name_exception_list != "":
                net_dev_exception_flag = f"--device-exceptions={net_device_if_name_exception_list}"
            command = f"python ../bin/xdnd-emulate-latency.py --device={net_device_if_name} --config={gigapaxos_config_filename} {net_dev_exception_flag}"
            print("   ", command)
            ret_code = os.system(command)
            assert ret_code == 0
            command = f"python ../bin/xdnd-emulate-latency.py --mode=verify --config={gigapaxos_config_filename}"
            print("   ", command)
            ret_code = os.system(command)
            assert ret_code == 0
            is_success = True
        except:
            time.sleep(10 ** (curr_attempt))
            curr_attempt += 1
        finally:
            assert curr_attempt < max_attempt, f'Failed to emulate and verify latency after {max_attempt} attempts'
    pass

def assign_clients_to_closest_replica(client_lists, replica_group_info, 
                                      server_address_by_name):
    """
    Add new field in client: 'TargetReplicaHostPort', example value: '10.10.1.1:2300'
    """

    for i, client in enumerate(client_lists):
        client_lat = client['Latitude']
        client_lon = client['Longitude']
        closest_replicas = find_k_closest_servers(replica_group_info['Replicas'],
                                                  {"Latitude": client_lat, "Longitude": client_lon}, 
                                                  1)
        assert len(closest_replicas) == 1
        closest_server_name = closest_replicas[0]['Name']
        assert closest_server_name in server_address_by_name, f"Unknown address for {closest_server_name}"
        target_address = server_address_by_name[closest_server_name]
        clients[i]['TargetReplicaHostPort'] = f"{target_address}:2300"
        clients[i]['TargetReplicaName'] = f"{closest_server_name}"

    return clients

# ==============================================================================

read_ratios = range(0, 101, 25)
consistency_models = [
    'causal', 'pram', 
    'monotonic_reads', 'writes_follow_reads', 'read_your_writes', 
    'monotonic_writes', 'eventual', 'sequential', 'linearizability']
consistency_models_abbr = {
    'linearizability': 'linearizable',
    'sequential': 'sequential',
    'causal': 'causal',
    'pram': 'pram',
    'monotonic_reads': 'mr',
    'writes_follow_reads': 'wfr',
    'read_your_writes': 'ryw',
    'monotonic_writes': 'mw',
    'eventual': 'eventual',
}

population_data_file = "location_distributions/client_us_metro_population.csv"
server_edge_location_file = "location_distributions/server_netflix_oca.csv"

result_filename = "results/latency_consistency.csv"
service_prop_template_file = "static/xdn_consistency_template.yaml"
service_endpoint = "/api/todo/tasks"
xdn_binary = "../bin/xdn"
num_request_per_loc = 10
num_clients = 100
random_seed = 313
num_tasks = 100
gp_config_file = "static/gigapaxos.xdn.3way.local.properties"
gp_config_template_file = "static/gigapaxos.xdn.cloudlab.template.properties"
num_cloudlab_machines = 11
control_plane_host = "10.10.1.12"
control_plane_http_port = "3300"
net_device_if_name="enp65s0f0np0"           # example: "ens1f1np1"
net_device_if_name_exception_list=""    # example: "10.10.1.6/enp130s0f0np0,10.10.1.8/enp4s0f0np0"
is_cache_docker_image=True
is_emulate_latency=True
is_run_numerical=True

# prepare the edge servers
nf_edge_server_locations = get_server_locations([server_edge_location_file], 
                                                remove_duplicate_location=True,
                                                remove_nonredundant_city_servers=False)
# prepare the clients
city_locations = get_client_locations(population_data_file)
population_ratio_per_city = get_population_ratio_per_city(city_locations)
client_count_per_city = get_client_count_per_city(population_ratio_per_city, num_clients, 0.0, city_locations[0]['City'])
clients = get_per_city_clients(client_count_per_city, city_locations)

# Replace dash with underscore as gigapaxos does not support dash in the server name
for i, server in enumerate(nf_edge_server_locations):
    nf_edge_server_locations[i]["Name"] = nf_edge_server_locations[i]["Name"].replace("-", "_")

# Set the seed for reproducibility
np.random.seed(random_seed)

# validate the number of machines
assert num_cloudlab_machines >= 11, f"This experiment needs to have at least 12 replica machines and 1 control plane machine"

# verify the required software
command = "ls xdn_latency_proxy_go/latency-injector"
ret_code = os.system(command)
assert ret_code == 0, f"cannot find the latency-injector program"
command = f"{xdn_binary} --help > /dev/null"
ret_code = os.system(command)
assert ret_code == 0, f"cannot find the xdn program"

# pull the used docker images in all machines, which is helpful to mitigate
# the pulls limit from Docker Hub.
if is_cache_docker_image:
    for i in range(num_cloudlab_machines):
        command = f'ssh 10.10.1.{i+1} "docker pull fadhilkurnia/xdn-todo:latest"'
        res = subprocess.run(command, shell=True, capture_output=True, text=True)
        assert res.returncode == 0, f"ERROR: {res.stdout}"
    command = f'ssh {control_plane_host} "docker pull fadhilkurnia/xdn-todo:latest"'
    res = subprocess.run(command, shell=True, capture_output=True, text=True)
    assert res.returncode == 0, f"ERROR: out={res.stdout} err={res.stderr}"

# write the data in a csv file
result_file = open(result_filename, "w")
result_file.write("consistency,read_ratio,avg_lat_ms,p50_lat_ms,stdev_lat\n")

for consistency in consistency_models:
    for read_ratio in read_ratios:

        # prepare the replica placement info
        protocol_class = 'eventual'
        if consistency == 'linearizability':
            protocol_class = consistency
        if consistency == "sequential":
            protocol_class = consistency
        replica_group_info = get_protocol_aware_replica_placement(protocol_class, clients, nf_edge_server_locations, read_ratio)
        curr_target_config_filename = f"static/gigapaxos.xdn.consistency.{consistency}_{read_ratio}.properties"
        curr_server_address_by_name = prepare_xdn_config_file(gp_config_template_file, 
                                                            curr_target_config_filename, 
                                                            replica_group_info, 
                                                            control_plane_host)
        
        # emulate and verify inter-server latency
        if is_emulate_latency:
            print(f">> Starting latency-injector proxy")
            emulate_inter_server_latency(curr_target_config_filename, 
                                        net_device_if_name, 
                                        net_device_if_name_exception_list)
        
        # generate graph of replica distribution
        target_map_figure_filename = f"plots/cons_replica_{consistency}_{read_ratio}.pdf"
        generate_replica_dist_map_figure(target_map_figure_filename, replica_group_info, clients=clients)

        if consistency == 'linearizability' or consistency == 'sequential':
            numerical_latencies = get_expected_latencies(replica_group_info['Replicas'],
                                                        clients,
                                                        replica_group_info['Leader']['Name'],
                                                        0.32258064516,
                                                        is_direct_all_to_leader=False)
            numerical_avg_ms = statistics.mean(numerical_latencies)
            print(f">>> {consistency}: {numerical_avg_ms} ms")

        print(f">> Running measurement for consistency={consistency} read_ratio={read_ratio}")

        # prepare list of boolean corresponds to the read ratio
        true_ratio = float(read_ratio)/100.0
        count_true = int(true_ratio * num_request_per_loc)
        count_false = num_request_per_loc - count_true
        bool_list = [True] * count_true + [False] * count_false
        random.shuffle(bool_list)
        is_read_list = bool_list

        # prepare the xdn cluster
        print(f">> Launching the XDN cluster ...")
        reset_xdn_cloudlab_cluster(num_cloudlab_machines, control_plane_host)
        consistency_abbr = consistency_models_abbr[consistency]
        screen_session_name = f"xdn_cons_{consistency_abbr}_{read_ratio}"
        start_xdn_cloudlab_cluster(screen_session_name, curr_target_config_filename)
        time.sleep(30)

        # start latency-injector proxy in the background in this driver machine
        print(f">> Starting latency-injector proxy ...")
        proxy_screen_session_name = f"scr_prx_cons_{consistency}_{read_ratio}"
        proxy_screen_log_filename = f"screen_logs/{proxy_screen_session_name}.log"
        os.system(f"fuser -s -k 8080/tcp")
        command = f"screen -L -Logfile {proxy_screen_log_filename} -S {proxy_screen_session_name} -d -m ./xdn_latency_proxy_go/latency-injector -config={curr_target_config_filename}"
        print("   ", command)
        ret_code = os.system(command)
        assert ret_code == 0
        request_proxies = {
            'http': 'http://127.0.0.1:8080',
            'https': 'http://127.0.0.1:8080',
        }

        # prepare client: assign to the closest replica, just like CDN
        clients = assign_clients_to_closest_replica(clients, 
                                                    replica_group_info, 
                                                    curr_server_address_by_name)
        for i, client in enumerate(clients):
            assert 'TargetReplicaHostPort' in client, f"Client: {client}"
            client_target_replica_host_port = client['TargetReplicaHostPort']
            clients[i]['ServiceTargetUrl'] = f"http://{client_target_replica_host_port}{service_endpoint}"

        # prepare the unique service name for this experiement configuration
        service_name = f"todo_{consistency_abbr}_{read_ratio}"

        # prepare the service property file
        print(f">> Preparing the service property file ...")
        service_prop_filename = f"static/xdn_cons_{consistency_abbr}.yaml"
        command = f"cp static/xdn_consistency_template.yaml {service_prop_filename}"
        ret_code = os.system(command)
        assert ret_code == 0
        replace_placeholder(service_prop_filename, "___SERVICE_NAME___", service_name)
        replace_placeholder(service_prop_filename, "___CONSITENCY_MODEL___", consistency)

        # deploy the service
        print(f">> Deploying the service ...")
        command = f"XDN_CONTROL_PLANE={control_plane_host} {xdn_binary} launch {service_name} --file={service_prop_filename}"
        max_attempt = 5; curr_attempt = 0; is_success = False
        while curr_attempt < max_attempt and not is_success:
            try:
                print(">>> ", command)
                result = subprocess.run(command, capture_output=True, text=True, shell=True)
                assert result.returncode == 0
                output = result.stdout
                assert "Error" not in output, f"output: {output}"
                is_success = True
            except Exception as e:
                print(f"  Failed to launch service {service_name}, error={repr(e)}:{e}, Retrying ...")
                time.sleep(10 ** (curr_attempt))
                curr_attempt += 1
            finally:
                assert curr_attempt < max_attempt, f'Failed to launch service {service_name} after {max_attempt} attempts'
        time.sleep(5)

        # reconfigure the leader location, if needed
        assert 'ServiceTargetUrl' in clients[0], f"client: {clients[0]}"
        warmup_service_target_url = clients[0]['ServiceTargetUrl']
        if consistency == "linearizability" or consistency == "sequential":
            leader_name = replica_group_info['Leader']['Name']
            replica_names = []
            for replica in replica_group_info["Replicas"]:
                replica_names.append(replica["Name"])
            command = f'curl -X POST http://{control_plane_host}:{control_plane_http_port}/api/v2/services/{service_name}/placement -d "{{"NODES" : {replica_names}, "COORDINATOR": "{leader_name}"}}"'
            print("   ", command)
            res = subprocess.run(command, shell=True, capture_output=True, text=True)
            assert res.returncode == 0
            output = res.stdout
            print("   ", output)
            output_json = json.loads(output)
            if "FAILED" in output_json:
                assert output_json["FAILED"] == False
            leader_address = curr_server_address_by_name[leader_name]
            warmup_service_target_url = f"http://{leader_address}:2300{service_endpoint}"   # target leader for warmup
            time.sleep(90)

            # verifying the leadership
            print(">> Verifying the leadership")
            for replica in replica_group_info["Replicas"]:
                replica_address = curr_server_address_by_name[replica['Name']]
                command = f'curl -X GET http://{replica_address}:2300/ -H "XDN: {service_name}" -H "XdnGetProtocolRoleRequest: true"'
                print("   ", command)
                res = subprocess.run(command, shell=True, capture_output=True, text=True)
                assert res.returncode == 0
                output = res.stdout
                print("   ", output)

        # warming up while populating the prepared data
        print(f">> Warming up ...")
        headers = {"Content-Type": "application/json", "XDN": service_name}
        post_data = []
        prev_cookie = None
        for i in range(num_tasks):
            post_data.append(f"{{\"item\":\"task-{i}\"}}")
        for i in range(num_tasks):
            try:
                response = requests.post(warmup_service_target_url, headers=headers, 
                                         data=post_data[i],
                                         timeout=1, cookies=prev_cookie)
                prev_cookie = response.cookies
            except Exception as e:
                print(f"Exception: {e}")

        # run measurement for each client in different city
        print(f">> Running measurements ...")
        latencies = []
        for client in clients:
            print(f">>  Curr client location: {client['City']}, count: {client['Count']}. replica: {client['TargetReplicaName']}")
            assert 'ServiceTargetUrl' in client, f"client: {client}"
            service_target_url = client['ServiceTargetUrl']
            
            # put client location in request header
            client_lat = client['Latitude']
            client_lon = client['Longitude']
            headers['X-Client-Location'] = f"{client_lat};{client_lon}"
            
            client_latencies = []
            for client_id in range(client['Count']):
                prev_cookie = None
                for i, is_read in enumerate(is_read_list):
                    start_time = time.perf_counter()
                    try:
                        response = None
                        if is_read:
                            response = requests.get(service_target_url, headers=headers, 
                                                    timeout=2, cookies=prev_cookie, proxies=request_proxies)
                        else:
                            response = requests.post(service_target_url, headers=headers, 
                                                    data=post_data[i % num_tasks],
                                                    timeout=2, cookies=prev_cookie, proxies=request_proxies)
                        prev_cookie = response.cookies
                    except Exception as e:
                        print(f"Exception: read={is_read} {e}")
                        print("url:", service_target_url, " is_read:", is_read, " cookie: ", prev_cookie, " headers: ", headers)
                    end_time = time.perf_counter()
                    latency = end_time - start_time
                    latency_ms = latency * 1_000.0
                    latencies.append(latency_ms)
                    client_latencies.append(latency_ms)
            client_avg_lat_ms = statistics.mean(latencies)
            print(f">>   Latency: {client_avg_lat_ms}")

        # gather the data
        avg_lat_ms = statistics.mean(latencies)
        med_lat_ms = statistics.median(latencies)
        stdev_lat_ms = statistics.stdev(latencies)
        print(f">> Result: consistency={consistency} read_ratio={read_ratio} avg_lat={avg_lat_ms:.2f}ms p50_lat={med_lat_ms}ms stddev={stdev_lat_ms}")
        print()
        result_file.write(f"{consistency},{read_ratio},{avg_lat_ms},{med_lat_ms},{stdev_lat_ms}\n")
        result_file.flush()

        time.sleep(5)

        # destroy the deployed service
        print(f">> Cleaning up ...")
        command = f"yes yes | XDN_CONTROL_PLANE={control_plane_host} {xdn_binary} service destroy {service_name}"
        try:
            print(">>> ", command)
            result = subprocess.run(command, capture_output=True, text=True, shell=True)
            print(result.stdout)
        except Exception as e:
            print(f"Error: {e}")
            print(e.stderr)

        # remove the XDN cluster
        command = f"../bin/gpServer.sh -DgigapaxosConfig={curr_target_config_filename} forceclear all > /dev/null 2>&1"
        print("   ", command)
        ret_code = os.system(command)
        assert ret_code == 0

        time.sleep(5)
        
        pass
    pass

result_file.close()
