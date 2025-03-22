import os
import re
import copy
import time
import json
import threading
import subprocess
from replica_group_picker import get_client_locations
from replica_group_picker import get_heuristic_replicas_placement
from replica_group_picker import find_k_closest_servers
from utils import get_population_ratio_per_city
from utils import get_client_count_per_city
from utils import get_uniform_client_per_city
from utils import get_server_locations
from utils import get_spanner_placement_menu
from utils import get_per_city_clients

population_data_file = "location_distributions/client_us_metro_population.csv"
server_edge_location_file = "location_distributions/server_netflix_oca.csv"
server_aws_location_file = "location_distributions/server_aws_region.csv"
server_gcp_location_file = "location_distributions/server_gcp_region.csv"
gigapaxos_template_config_file = "../conf/gigapaxos.xdnlat.template.properties"
request_payload_file="measurement_payload.json"

geolocalities = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.2, 0.0]
approaches = ["XDNNR", "ED", "GD", "XDN", "CD"]
num_services = 50
num_replicas = 3
num_clients = 1000
num_req_per_client = 10
city_area_sqkm = 1000
net_device_if_name="enp3s0f0np0"        # example: "ens1f1np1"
net_device_if_name_exception_list=""    # example: "10.10.1.6/enp130s0f0np0,10.10.1.8/enp4s0f0np0"
service_name="bookcatalog"
request_endpoint="/api/books"
num_cloudlab_machines = 10
control_plane_address="10.10.1.11"
control_plane_http_port="3300"
static_nr_server_name="us-east-1"
enable_city_parallelism=True
enable_inter_city_lat_emulation=True
is_sample_city=True
is_spread_city_client=False
is_cache_docker_image=True

results_base_dir="/mydata/latency-results"
os.makedirs(results_base_dir, exist_ok=True)
os.makedirs("screen_logs", exist_ok=True)

city_locations = get_client_locations(population_data_file)
population_ratio_per_city = get_population_ratio_per_city(city_locations)
aws_server_locations = get_server_locations([server_aws_location_file])
gcp_server_locations = get_server_locations([server_gcp_location_file])
nf_edge_server_locations = get_server_locations([server_edge_location_file], 
                                                remove_duplicate_location=False,
                                                remove_nonredundant_city_servers=True)

# replace dash with underscore as gigapaxos does not support dash in the server name
for i, server in enumerate(nf_edge_server_locations):
    nf_edge_server_locations[i]["Name"] = nf_edge_server_locations[i]["Name"].replace("-", "_")
for i, server in enumerate(aws_server_locations):
    aws_server_locations[i]["Name"] = aws_server_locations[i]["Name"].replace("-", "_")
gcp_og_server_locations = copy.deepcopy(gcp_server_locations)
for i, server in enumerate(gcp_server_locations):
    gcp_server_locations[i]["Name"] = gcp_server_locations[i]["Name"].replace("-", "_")
static_nr_server_name = static_nr_server_name.replace("-", "_")

# initialize static server for NR / CD
static_nr_server=None
for server in aws_server_locations:
    if server["Name"] == static_nr_server_name:
        static_nr_server = server
        break
assert static_nr_server != None

# initialize spanner placement menu
spanner_placement_menu_info = get_spanner_placement_menu(gcp_og_server_locations)
spanner_placement_menu = spanner_placement_menu_info["PlacementMenu"]
spanner_placement_menu_per_leader = spanner_placement_menu_info["PlacementMenuByLeader"]

# deduplicates menu in spanner with same region name
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
    replicas[0]["Name"] = replicas[0]["Name"] + "_1"
    replicas[1]["Name"] = replicas[1]["Name"] + "_2"
    spanner_replica_group_by_menu[menu] = {"Replicas": replicas, "Leader": replicas[0].copy()}
spanner_leader_locations = []
for leader_name, menu in spanner_placement_menu_per_leader.items():
    server = gcp_og_server_by_name[leader_name]
    spanner_leader_locations.append(server.copy())

# verifying the required software
command = "ls xdn_latency_proxy_go/latency-injector"
ret_code = os.system(command)
assert ret_code == 0, f"cannot find the latency-injector program"
command = "xdn --help > /dev/null"
ret_code = os.system(command)
assert ret_code == 0, f"cannot find the xdn program"

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


# begin measurement with all the parameters
for approach in approaches:
    approach_lc = approach.lower()
    approach_num_replicas = num_replicas
    
    # adjust batch size to maximize parellilism for approaches with no replication
    if approach == "CD" or approach == "ED" or approach == "XDNNR":
        approach_num_replicas = 1
    num_service_per_batch = num_cloudlab_machines // approach_num_replicas
    
    for geolocality in geolocalities:
        
        # The high level algorithm:
        # - Select 10 server locations based on client distributions
        # - Replace Gigapaxos config to use the 10 selected server locations.
        # - Emulate inter-server latency for the selected 10 server.
        # - Start latency-injector proxy.
        # - Start warmup workload using ab.
        # - Start measured sequential request with ab.

        #  groups cities into batch, due to the limited number of machines
        city_list = list(population_ratio_per_city.keys())
        if is_sample_city:
            city_list = city_list[0:3] + city_list[23:26] + city_list[47:50]
        batch_of_cities = [city_list[i:i + num_service_per_batch] for i in range(0, len(city_list), num_service_per_batch)]

        for batch_id, city_batch in enumerate(batch_of_cities):
            print(f">> preparing latency measurement with approach={approach_lc} g={geolocality} batch_id={batch_id} localities={city_batch}:")
            screen_session_base_name=f"scr_{approach_lc}_g{geolocality}_b{batch_id}"
            
            client_distributions_per_city = {}
            replica_group_info_per_city = {}
            for idx, city_name in enumerate(city_batch):
                # distribute the clients in all cities
                client_count_per_city = get_client_count_per_city(population_ratio_per_city, num_clients, geolocality, city_name)
                clients = []
                if is_spread_city_client:
                    clients = get_uniform_client_per_city(client_count_per_city, city_locations, city_area_sqkm)
                else:
                    clients = get_per_city_clients(client_count_per_city, city_locations)
                client_distributions_per_city[city_name] = clients
                
                # pick server location
                replica_group_info = {}
                if approach == "XDN":
                    # xdn with 3 replicas at the edge
                    replica_group_info = get_heuristic_replicas_placement(nf_edge_server_locations, clients, num_replicas, True)
                    replica_group_info_per_city[city_name] = replica_group_info
                elif approach == "XDNNR":
                    # xdn with 1 replica at the edge
                    replica_group_info = get_heuristic_replicas_placement(nf_edge_server_locations, clients, 1, True)
                    replica_group_info_per_city[city_name] = replica_group_info
                elif approach == "CD":
                    # xdn with 1 replica in aws us-east-1, we artificially use us-east-1-1, us-east-1-2, us-east-1-3, ...
                    curr_city_counter = idx + 1
                    curr_global_city_counter = batch_id * num_service_per_batch + curr_city_counter
                    curr_static_nr_server = static_nr_server.copy()
                    curr_static_nr_server["Name"] = f"{static_nr_server['Name']}_{curr_global_city_counter}"
                    replica_group_info = {"Replicas": [curr_static_nr_server], "Leader": curr_static_nr_server}
                    replica_group_info_per_city[city_name] = replica_group_info
                elif approach == "ED":
                    # deploy cloudflare durable object in one edge server.
                    # we add id suffix in the server name, mitigating conflicting replica placement.
                    curr_city_counter = idx + 1
                    curr_global_city_counter = batch_id * num_service_per_batch + curr_city_counter
                    replica_group_info = get_heuristic_replicas_placement(nf_edge_server_locations, clients, 1, True)
                    assert len(replica_group_info["Replicas"]) == 1
                    updated_replica_group_info = copy.deepcopy(replica_group_info)
                    updated_replica_group_info["Leader"]["Name"] = f"{replica_group_info['Leader']['Name']}_{curr_global_city_counter}"
                    for i, replica in enumerate(updated_replica_group_info['Replicas']):
                        updated_replica_group_info["Replicas"][i]["Name"] = f"{updated_replica_group_info['Replicas'][i]['Name']}_{curr_global_city_counter}"
                    replica_group_info_per_city[city_name] = updated_replica_group_info
                elif approach == "GD":
                    # support other approaches. deploy rqlite
                    # we artificially use global_city_counter prefix to prevent conflict with other replica group
                    curr_city_counter = idx + 1
                    curr_global_city_counter = batch_id * num_service_per_batch + curr_city_counter
                    leader_candidate = get_heuristic_replicas_placement(spanner_leader_locations, clients, 1, True)
                    placement_menu_candidate = spanner_placement_menu_per_leader[leader_candidate["Leader"]["Name"]]
                    replica_group_info = copy.deepcopy(spanner_replica_group_by_menu[placement_menu_candidate])
                    replica_group_info["Leader"]["Name"] = f"{replica_group_info['Leader']['Name']}_{curr_global_city_counter}"
                    for i, replica in enumerate(replica_group_info['Replicas']):
                        replica_group_info["Replicas"][i]["Name"] = f"{replica_group_info['Replicas'][i]['Name']}_{curr_global_city_counter}"
                    replica_group_info_per_city[city_name] = replica_group_info
                else:
                    raise Exception(f"Unknown approach of {approach}")
            
            # gather the required server names in this batch
            server_set = set()
            server_by_name = {}
            for city_name, replica_group_info in replica_group_info_per_city.items():
                for replica in replica_group_info["Replicas"]:
                    server_set.add(replica["Name"])
                    server_by_name[replica["Name"]] = replica

            # prepare config string for this batch, including assigning address for each server name
            server_address_by_name = {}
            machine_counter = 1
            server_name_address=""
            server_name_geolocation=""
            for server_name in server_set:
                server = server_by_name[server_name]
                server_name_address += f"active.{server_name}=10.10.1.{machine_counter}:2000\n"
                server_name_geolocation += f"active.{server_name}.geolocation=\"{server['Latitude']},{server['Longitude']}\"\n"
                server_address_by_name[server_name] = f"10.10.1.{machine_counter}"
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
            updated_cfg_file = f"../conf/{approach_lc}-{geolocality}-{batch_id}-lat.properties"
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
            proxy_screen_name = screen_session_base_name + "_prx"
            print(f" > starting client's latency injector, location_config={updated_cfg_file}")
            command = f"fuser -s -k 8080/tcp"
            print("   ", command)
            os.system(command)
            command = f"screen -XS {proxy_screen_name} quit"
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
                command = f"screen -XS {gp_screen_name} quit"
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

            # Iterates over locality (i.e., city)
            print(" > start measurement over different service locality (city), in parallel")          
            # measure different service in different thread, if enabled
            measurement_threads = []
            curr_city_counter = 0
            for city_name in city_batch:
                curr_city_counter += 1
                global_city_counter = batch_id * num_service_per_batch + curr_city_counter
                
                def run_geo_lat_measurement(approach, city_name, global_city_counter, replica_group_info, clients):

                    server_name_address_pairs = []
                    for server in replica_group_info["Replicas"]:
                        server_name = server["Name"]
                        server_address = server_address_by_name[server_name]
                        temp = f"{server_name}/{server_address}"
                        server_name_address_pairs.append(temp)

                    # deploy the service
                    print(f"   > deploying local service based in {city_name}")
                    deployed_service_name=f"bookcatalog{global_city_counter:03}"
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

                    # reconfigure the leader
                    leader_name = replica_group_info["Leader"]["Name"]
                    leader_address = server_address_by_name[leader_name]
                    if approach == "XDN" or approach == "CD" or approach == "XDNNR":
                        # we dont need to do this if we only have 1 active.
                        if len(server_set) != 1:
                            replica_names = []
                            for replica in replica_group_info["Replicas"]:
                                replica_names.append(replica["Name"])
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
                        replica = replica_group_info["Leader"]
                        address = server_address_by_name[replica["Name"]]
                        locality_name = city_name.replace(" ", "")
                        log_filename = f'{screen_session_base_name}_l{locality_name}.log'
                        command = f'ssh {address} "cd xdn/eval/durable_objects/bookcatalog && npm install > /dev/null 2>&1 && fuser -k 2300/tcp"'
                        ret_code = os.system(command)
                        command = f'ssh {address} "cd xdn/eval/durable_objects/bookcatalog && nohup npx wrangler dev --ip {address} --port 2300 > {log_filename}" &'
                        print("   ", command)
                        ret_code = os.system(command)
                        assert ret_code == 0
                        time.sleep(5)

                    # get the closest replica for each client
                    target_replica_by_cid = {}
                    for client in clients:
                        client_lat = float(client["Latitude"])
                        client_lon = float(client["Longitude"])
                        closest_servers = find_k_closest_servers(replica_group_info["Replicas"], {"Latitude": client_lat, "Longitude": client_lon}, 1)
                        assert len(closest_servers) == 1
                        closest_server_name = closest_servers[0]['Name']
                        target_address = server_address_by_name[closest_server_name]
                        client_id = client["ClientID"]
                        target_replica_by_cid[client_id] = target_address

                    time.sleep(5)

                    # start warmup workload using ab
                    locality_name = city_name.replace(" ", "")
                    target_latency_file = f"{results_base_dir}/{approach_lc}_latency_g{geolocality}_b{batch_id}_s{locality_name}_warmup.tsv"
                    command = f"ab -X 127.0.0.1:8080 -k -g {target_latency_file} -p {request_payload_file} -T application/json -H 'XDN: {deployed_service_name}' -c 3 -n 1000 http://{leader_address}:2300{request_endpoint} 2>/dev/null | grep \"Time per request:\""
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

                    # start measurement using ab
                    for client in clients:
                        print(f"       > measuring from client based in {client['City']}: {server_name_address_pairs}")
                        client_id = client["ClientID"]
                        client_lat = float(client["Latitude"])
                        client_lon = float(client["Longitude"])
                        target_address = target_replica_by_cid[client_id]
                        client_name = client["City"].replace(" ", "")
                        target_latency_file = f"{results_base_dir}/{approach_lc}_latency_g{geolocality}_b{batch_id}_s{locality_name}_c{client_name}_i{client_id:04d}.tsv"

                        num_requests = num_req_per_client * int(client['Count'])
                        command = f"ab -X 127.0.0.1:8080 -k -g {target_latency_file} -p {request_payload_file} -T application/json -H 'XDN: {deployed_service_name}' -H 'X-Client-Location: {client_lat};{client_lon}' -c 1 -n {num_requests} http://{target_address}:2300{request_endpoint} 2>/dev/null | grep \"Time per request:\""
                        print("   ", command)
                        max_repetitions = 3; num_attempt = 0; ret_code = 0
                        while num_attempt < max_repetitions:
                            ret_code = os.system(command)
                            if ret_code == 0:
                                break
                            num_attempt += 1
                            if num_attempt < max_repetitions:
                                time.sleep(10 ** (num_attempt))
                        assert ret_code == 0, f"Command failed after {max_repetitions} attempts."

                    # destroy the service
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
                            locality_name = city_name.replace(" ", "")
                            log_filename = f'{screen_session_base_name}_l{locality_name}.log'
                            command = f'scp -q -o LogLevel=QUIET {address}:~/xdn/eval/durable_objects/bookcatalog/{log_filename} screen_logs/{log_filename} && ssh {address} "rm -rf xdn/eval/durable_objects/bookcatalog/{log_filename}"'
                            print("   ", command)
                            ret_code = os.system(command)
                            assert ret_code == 0
                    
                    return
    
                curr_thread = threading.Thread(target=run_geo_lat_measurement, args=(approach,
                                                                                     city_name, 
                                                                                     global_city_counter, 
                                                                                     replica_group_info_per_city[city_name],
                                                                                     client_distributions_per_city[city_name]))
                
                curr_thread.start()
                measurement_threads.append(curr_thread)
                if not enable_city_parallelism:
                    curr_thread.join()

                time.sleep(2)
                continue
            
            # wait until all threads are done
            for thread in measurement_threads:
                thread.join()
                continue

            print(f" > removing the xdn cluster")
            command = f"../bin/gpServer.sh -DgigapaxosConfig={updated_cfg_file} forceclear all > /dev/null 2>&1"
            print("   ", command)
            ret_code = os.system(command)
            assert ret_code == 0

            time.sleep(60)
