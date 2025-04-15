import os
import csv
import json
import time
import requests
import subprocess
import statistics
from utils import replace_placeholder
from utils import generate_random_string

approaches = ['pb', 'rsm']
req_sizes = [8, 32, 64, 256, 512, 1024, 2048, 4086, 8192, 16384, 32768, 65536, 131072]
statediff_sizes = [8, 32, 64, 256, 512, 1024, 2048, 4086, 8192, 16384, 32768, 65536, 131072]
exec_times = [2, 4, 8, 16, 32, 64, 128, 256, 512]

control_plane_address = "10.10.1.4"
control_plane_http_port = "3300"
result_filename = "results/latency_rsm_pb.csv"
xdn_binary = "../bin/xdn"
docker_image = "fadhilkurnia/xdn-rsmbench"
num_repetitions = 1_000
gp_config_file = "static/gigapaxos.xdn.3way.cloudlab.properties"
num_machines = 3
is_cache_docker_image = False
default_leader_address = "10.10.1.2"

# validate xdn binary does exist
command = f"{xdn_binary} --help > /dev/null"
ret_code = os.system(command)
assert ret_code == 0, "Cannot find the xdn cli."

def clear_xdn_cluster():
    # clear any remaining running xdn or other processes
    print(f" > resetting the measurement cluster:")
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

def run_xdn_cluster(gp_config_file, screen_session_name):
    clear_xdn_cluster()

    # deploy XDN cluster, store the log output in screen_logs
    print(f" > deploying the measurement cluster:")
    screen_log_filename = f"screen_logs/{screen_session_name}.log"
    os.system(f"rm -f {screen_log_filename}")
    command = f"screen -L -Logfile screen_logs/{screen_session_name}.log -S {screen_session_name} -d -m bash -c '../bin/gpServer.sh -DgigapaxosConfig={gp_config_file} start all; exec bash'"
    print("   ", command)
    ret_code = os.system(command)
    assert ret_code == 0
    time.sleep(20)

# pull the used docker images in all machines, which is helpful to mitigate
# the pulls limit from Docker Hub.
if is_cache_docker_image:
    for i in range(num_machines):
        command = f'ssh 10.10.1.{i+1} "docker pull fadhilkurnia/xdn-rsmbench:latest && docker pull busybox"'
        res = subprocess.run(command, shell=True, capture_output=True, text=True)
        assert res.returncode == 0, f"ERROR: {res.stdout}"
    command = f'ssh {control_plane_address} "docker pull fadhilkurnia/xdn-rsmbench:latest && docker pull busybox"'
    res = subprocess.run(command, shell=True, capture_output=True, text=True)
    assert res.returncode == 0, f"ERROR: out={res.stdout} err={res.stderr}"

result_file = open(result_filename, "w")
result_file.write("measurement,approach,req_size,exec_time,statediff_size,avg_lat_ms,p50_lat_ms,stdev_lat\n")

# vary the request sizes
print(">>> Vary request size ...")
for approach in approaches:
    for req_size in req_sizes:

        print(f" >> Handling approach={approach} req_size={req_size}")

        # deploy XDN cluster, store the log output in screen_logs
        screen_session_name = f"xdn_lat_rsm_pb_vary_req_size_{req_size}_{approach}"
        run_xdn_cluster(gp_config_file, screen_session_name)

        # deploy the service
        is_deterministic = "true"
        if approach == "pb":
            is_deterministic = "false"
        service_name = f"rsmbench_vrs_{approach}_{req_size}"
        target_conf_filename = f"static/xdn_vrs.yaml"
        command = f"cp static/xdn_rsm_pb_template.yaml {target_conf_filename}"
        ret_code = os.system(command=command)
        assert ret_code == 0
        replace_placeholder(target_conf_filename, "___SERVICE_NAME___", service_name)
        replace_placeholder(target_conf_filename, "___IS_DETERMINISTIC___", is_deterministic)
        replace_placeholder(target_conf_filename, "___EXEC_TIME___", "1")
        replace_placeholder(target_conf_filename, "___STATEDIFF_SIZE_BYTES___", "8")
        command = f"XDN_CONTROL_PLANE={control_plane_address} {xdn_binary} launch {service_name} --file=static/xdn_vrs.yaml"
        try:
            print(">>> ", command)
            result = subprocess.run(command, capture_output=True, text=True, shell=True)
            assert result.returncode == 0
        except Exception as e:
            print(f"Error executing command: {e}")
            print(e.stderr)
        time.sleep(10)

        # reconfigure the leader
        print(">>> reconfiguring the leader ...")
        leader_address = default_leader_address
        if approach == "rsm":
            try:
                for i in range(num_machines):
                    response = requests.get(f"http://10.10.1.{i+1}:2300/", headers={"XdnGetProtocolRoleRequest": "true", "XDN": service_name}, timeout=1)
                    response_json = json.loads(response.text)
                    if "role" in response_json and response_json["role"] == "leader":
                        leader_address = f"10.10.1.{i+1}"
                        print("detected leader: ", leader_address)
                        break
                time.sleep(5)
            except Exception as e:
                print(f"Exception: {e}")
        if approach == "pb":
            try:
                for i in range(num_machines):
                    response = requests.get(f"http://10.10.1.{i+1}:2300/", headers={"XdnGetProtocolRoleRequest": "true", "XDN": service_name}, timeout=1)
                    response_json = json.loads(response.text)
                    if "role" in response_json and response_json["role"] == "primary":
                        leader_address = f"10.10.1.{i+1}"
                        print("detected primary: ", leader_address)
                        break
                time.sleep(5)
            except Exception as e:
                print(f"Exception: {e}")

        service_endpoint = f"http://{leader_address}:2300/"
        headers = {"XDN": service_name}

        # save the POST request payload to external file
        post_req_payload_filename = f"static/rsm_pb_payload_{req_size}.txt"
        post_data = generate_random_string(req_size)
        post_data_file = open(post_req_payload_filename, "w")
        post_data_file.write(post_data)
        post_data_file.close()

        # run the warmup
        print(">>> warming up ...")
        for i in range(100):
            try:
                response = requests.post(service_endpoint, headers=headers, 
                                         data=post_data, timeout=3)
            except Exception as e:
                print(f"Exception: {e}")

        # run the actual measurements
        print(">>> running measurements ...")
        directory_path = "results_lat_rsm_pb"
        os.makedirs(directory_path, exist_ok=True)
        target_latency_filename = f"results_lat_rsm_pb/rsm_pb_raw_lat_vrs_{req_size}_{approach}.tsv"
        command = f"ab -k -p {post_req_payload_filename} -g {target_latency_filename} -T text/plain -H 'XDN: {service_name}' -c 1 -n {num_repetitions} {service_endpoint} > /dev/null"
        print("   ", command)
        ret_code = os.system(command)
        assert ret_code == 0

        # read the produced raw latency data
        latencies = []
        with open(target_latency_filename, 'r') as tsvfile:
            tsv_reader = csv.DictReader(tsvfile, delimiter='\t')
            for row in tsv_reader:
                latencies.append(float(row["ttime"]))

        avg_lat_ms = statistics.mean(latencies)
        med_lat_ms = statistics.median(latencies)
        stdev_lat_ms = statistics.stdev(latencies)
        print(f"Approach: {approach} \tReq Size: {req_size} \tAvg.Latency: {avg_lat_ms:.2f} ms \t Med.Latency: {med_lat_ms:.2f}")
        result_file.write(f"vary_req_size,{approach},{req_size},1,8,{avg_lat_ms},{med_lat_ms},{stdev_lat_ms}\n")
        result_file.flush()

        # destroy the deployed service
        command = f"yes yes | XDN_CONTROL_PLANE={control_plane_address} {xdn_binary} service destroy {service_name}"
        try:
            print(">>> ", command)
            result = subprocess.run(command, capture_output=True, text=True, shell=True)
            print(result.stdout)
            time.sleep(3)
        except Exception as e:
            print(f"Error executing command: {e}")
            print(e.stderr)

        # destroy the XDN cluster
        clear_xdn_cluster()
        time.sleep(3)
        
        pass

time.sleep(10)

# vary statediff size
print(">>> Vary statediff size ...")
for approach in approaches:
    for statediff_size in statediff_sizes:
        print(f" >> Handling approach={approach} statediff_size={statediff_size}")

        # deploy XDN cluster, store the log output in screen_logs
        screen_session_name = f"xdn_lat_rsm_pb_vary_sd_size_{statediff_size}_{approach}"
        run_xdn_cluster(gp_config_file, screen_session_name)
    
        # deploy the service
        is_deterministic = "true"
        if approach == "pb":
            is_deterministic = "false"
        service_name = f"rsmbench_vsd_{approach}_{statediff_size}"
        target_conf_filename = f"static/xdn_vsd.yaml"
        command = f"cp static/xdn_rsm_pb_template.yaml {target_conf_filename}"
        ret_code = os.system(command=command)
        assert ret_code == 0
        replace_placeholder(target_conf_filename, "___SERVICE_NAME___", service_name)
        replace_placeholder(target_conf_filename, "___IS_DETERMINISTIC___", is_deterministic)
        replace_placeholder(target_conf_filename, "___EXEC_TIME___", "1")
        replace_placeholder(target_conf_filename, "___STATEDIFF_SIZE_BYTES___", f"{statediff_size}")
        command = f"XDN_CONTROL_PLANE={control_plane_address} {xdn_binary} launch {service_name} --file=static/xdn_vsd.yaml"
        try:
            print(">>> ", command)
            result = subprocess.run(command, capture_output=True, text=True, shell=True)
            assert result.returncode == 0
        except Exception as e:
            print(f"Error executing command: {e}")
            print(e.stderr)
        time.sleep(10)

        # reconfigure the leader
        print(">>> reconfiguring the leader ...")
        leader_address = default_leader_address
        if approach == "rsm":
            try:
                for i in range(num_machines):
                    response = requests.get(f"http://10.10.1.{i+1}:2300/", headers={"XdnGetProtocolRoleRequest": "true", "XDN": service_name}, timeout=1)
                    response_json = json.loads(response.text)
                    if "role" in response_json and response_json["role"] == "leader":
                        leader_address = f"10.10.1.{i+1}"
                        print("detected leader: ", leader_address)
                        break
                time.sleep(5)
            except Exception as e:
                print(f"Exception: {e}")
        if approach == "pb":
            try:
                for i in range(num_machines):
                    response = requests.get(f"http://10.10.1.{i+1}:2300/", headers={"XdnGetProtocolRoleRequest": "true", "XDN": service_name}, timeout=1)
                    response_json = json.loads(response.text)
                    if "role" in response_json and response_json["role"] == "primary":
                        leader_address = f"10.10.1.{i+1}"
                        print("detected primary: ", leader_address)
                        break
                time.sleep(5)
            except Exception as e:
                print(f"Exception: {e}")

        service_endpoint = f"http://{leader_address}:2300/"
        headers = {"XDN": service_name}

        # save the POST request payload to external file
        post_req_payload_filename = f"static/rsm_pb_payload_8.txt"
        post_data = generate_random_string(8)
        post_data_file = open(post_req_payload_filename, "w")
        post_data_file.write(post_data)
        post_data_file.close()

        # run the warmup
        for i in range(100):
            try:
                response = requests.post(service_endpoint, headers=headers, 
                                         data=post_data, timeout=3)
            except Exception as e:
                print(f"Exception: {e}")

        # run the actual measurements
        print(">>> running measurements ...")
        directory_path = "results_lat_rsm_pb"
        os.makedirs(directory_path, exist_ok=True)
        target_latency_filename = f"results_lat_rsm_pb/rsm_pb_raw_lat_vsd_{statediff_size}_{approach}.tsv"
        command = f"ab -k -p {post_req_payload_filename} -g {target_latency_filename} -T text/plain -H 'XDN: {service_name}' -c 1 -n {num_repetitions} {service_endpoint} > /dev/null"
        print("   ", command)
        ret_code = os.system(command)
        assert ret_code == 0

        # read the produced raw latency data
        latencies = []
        with open(target_latency_filename, 'r') as tsvfile:
            tsv_reader = csv.DictReader(tsvfile, delimiter='\t')
            for row in tsv_reader:
                latencies.append(float(row["ttime"]))

        avg_lat_ms = statistics.mean(latencies)
        med_lat_ms = statistics.median(latencies)
        stdev_lat_ms = statistics.stdev(latencies)
        print(f"Approach: {approach} \tStatediff Size: {statediff_size} \tAvg.Latency: {avg_lat_ms:.2f} ms \t Med.Latency: {med_lat_ms:.2f}")
        result_file.write(f"vary_statediff_size,{approach},8,1,{statediff_size},{avg_lat_ms},{med_lat_ms},{stdev_lat_ms}\n")
        result_file.flush()

        # destroy the deployed service
        command = f"yes yes | XDN_CONTROL_PLANE={control_plane_address} {xdn_binary} service destroy {service_name}"
        try:
            print(">>> ", command)
            result = subprocess.run(command, capture_output=True, text=True, shell=True)
            print(result.stdout)
            time.sleep(3)
        except Exception as e:
            print(f"Error executing command: {e}")
            print(e.stderr)

        # destroy the XDN cluster
        clear_xdn_cluster()
        time.sleep(3)
        pass

time.sleep(10)

# vary exec time
print(">>> Vary execution time ...")
for approach in approaches:
    for exec_time in exec_times:
        print(f" >> Handling approach={approach} exec_time={exec_time}")

        # deploy XDN cluster, store the log output in screen_logs
        screen_session_name = f"xdn_lat_rsm_pb_vary_exec_time_{exec_time}_{approach}"
        run_xdn_cluster(gp_config_file, screen_session_name)

        # deploy the service
        is_deterministic = "true"
        if approach == "pb":
            is_deterministic = "false"
        service_name = f"rsmbench_vet_{approach}_{exec_time}"
        target_conf_filename = f"static/xdn_vet.yaml"
        command = f"cp static/xdn_rsm_pb_template.yaml {target_conf_filename}"
        ret_code = os.system(command=command)
        assert ret_code == 0
        replace_placeholder(target_conf_filename, "___SERVICE_NAME___", service_name)
        replace_placeholder(target_conf_filename, "___IS_DETERMINISTIC___", is_deterministic)
        replace_placeholder(target_conf_filename, "___EXEC_TIME___", f"{exec_time}")
        replace_placeholder(target_conf_filename, "___STATEDIFF_SIZE_BYTES___", "8")
        command = f"XDN_CONTROL_PLANE={control_plane_address} {xdn_binary} launch {service_name} --file=static/xdn_vet.yaml"
        try:
            print(">>> ", command)
            result = subprocess.run(command, capture_output=True, text=True, shell=True)
            assert result.returncode == 0
        except Exception as e:
            print(f"Error executing command: {e}")
            print(e.stderr)
        time.sleep(10)

        # reconfigure the leader
        print(">>> reconfiguring the leader ...")
        leader_address = default_leader_address
        if approach == "rsm":
            try:
                for i in range(num_machines):
                    response = requests.get(f"http://10.10.1.{i+1}:2300/", headers={"XdnGetProtocolRoleRequest": "true", "XDN": service_name}, timeout=1)
                    response_json = json.loads(response.text)
                    if "role" in response_json and response_json["role"] == "leader":
                        leader_address = f"10.10.1.{i+1}"
                        print("detected leader: ", leader_address)
                        break
                time.sleep(5)
            except Exception as e:
                print(f"Exception: {e}")
        if approach == "pb":
            try:
                for i in range(num_machines):
                    response = requests.get(f"http://10.10.1.{i+1}:2300/", headers={"XdnGetProtocolRoleRequest": "true", "XDN": service_name}, timeout=1)
                    response_json = json.loads(response.text)
                    if "role" in response_json and response_json["role"] == "primary":
                        leader_address = f"10.10.1.{i+1}"
                        print("detected primary: ", leader_address)
                        break
                time.sleep(5)
            except Exception as e:
                print(f"Exception: {e}")

        service_endpoint = f"http://{leader_address}:2300/"
        headers = {"XDN": service_name}

        # save the POST request payload to external file
        post_req_payload_filename = f"static/rsm_pb_payload_8.txt"
        post_data = generate_random_string(8)
        post_data_file = open(post_req_payload_filename, "w")
        post_data_file.write(post_data)
        post_data_file.close()

        # run the warmup
        for i in range(100):
            try:
                response = requests.post(service_endpoint, headers=headers, 
                                         data=post_data, timeout=3)
            except Exception as e:
                print(f"Exception: {e}")

        # run the actual measurements
        print(">>> running measurements ...")
        directory_path = "results_lat_rsm_pb"
        os.makedirs(directory_path, exist_ok=True)
        target_latency_filename = f"results_lat_rsm_pb/rsm_pb_raw_lat_vet_{exec_time}_{approach}.tsv"
        command = f"ab -k -p {post_req_payload_filename} -g {target_latency_filename} -T text/plain -H 'XDN: {service_name}' -c 1 -n {num_repetitions} {service_endpoint} > /dev/null"
        print("   ", command)
        ret_code = os.system(command)
        assert ret_code == 0

        # read the produced raw latency data
        latencies = []
        with open(target_latency_filename, 'r') as tsvfile:
            tsv_reader = csv.DictReader(tsvfile, delimiter='\t')
            for row in tsv_reader:
                latencies.append(float(row["ttime"]))

        avg_lat_ms = statistics.mean(latencies)
        med_lat_ms = statistics.median(latencies)
        stdev_lat_ms = statistics.stdev(latencies)
        print(f"Approach: {approach} \tExec Time: {exec_time} ms \tAvg.Latency: {avg_lat_ms:.2f} ms \t Med.Latency: {med_lat_ms:.2f}")
        result_file.write(f"vary_exec_time,{approach},8,{exec_time},8,{avg_lat_ms},{med_lat_ms},{stdev_lat_ms}\n")
        result_file.flush()

        # destroy the deployed service
        command = f"yes yes | XDN_CONTROL_PLANE={control_plane_address} {xdn_binary} service destroy {service_name}"
        try:
            print(">>> ", command)
            result = subprocess.run(command, capture_output=True, text=True, shell=True)
            print(result.stdout)
            time.sleep(3)
        except Exception as e:
            print(f"Error executing command: {e}")
            print(e.stderr)

        # destroy the XDN cluster
        clear_xdn_cluster()
        time.sleep(3)
        pass

# store the result in the output file
result_file.close()
