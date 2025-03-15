# xdnd-emulate-latency.py
#
# Script to emulate latency based on the geolocation data provided in the
# gigapaxos properties file. We emulate latency by injecting it using tcconfig.

import time
import math
import threading
import subprocess

DEFAULT_NET_DEV_INTERFACE_NAME = 'eth1'
DEFAULT_CONFIG_FILE = 'conf/gigapaxos.cloudlab-virtual.properties'
MINIMUM_INTER_SERVER_RTT_LATENCY_MS=1

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points on the Earth 
    using the haversine formula. Returns distance in kilometers.
    """
    # Earth radius in kilometers
    R = 6371.0

    # Convert degrees to radians
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = ((math.sin(dphi / 2) ** 2) 
         + math.cos(phi1) * math.cos(phi2) * (math.sin(dlambda / 2) ** 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Distance in kilometers
    distance = R * c
    return distance

def read_servers_from_property_file(filename: str) -> dict:
    """
    Reads a property file with lines like:
      active.aa="192.168.1.10"
      active.aa.geolocation="39.5832,-82.3511"
    and returns a dictionary of the form:
      {
         "aa": {
             "ip": "192.168.1.10",
             "geolocation": (39.5832, -82.3511)
         },
         ...
      }
    """
    servers = {}
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            line = line.partition('#')[0]

            # Skip empty lines or commented lines if any
            if not line or line.startswith("#"):
                continue

            # Only consider lines for server location or host
            if not line.startswith("active"):
                continue

            # Handle config for server location.
            # Each line should look like active.xx.geolocation="lat,lon"
            if "geolocation" in line:
                parts = line.split("=")
                
                # Skip malformed lines
                if len(parts) != 2:
                    continue

                key = parts[0].strip()
                value = parts[1].strip().strip('"')

                # Skip malformed key
                key_parts = key.split('.')
                if len(key_parts) < 3:
                    continue

                # Get server name, e.g., 'aa' from 'active.aa.geolocation'
                server_name = key_parts[1]

                # value is something like '39.5832,-82.3511'
                # split by comma to get lat & lon
                coords = value.split(',')
                if len(coords) != 2:
                    continue

                lat_str, lon_str = coords
                try:
                    lat = float(lat_str)
                    lon = float(lon_str)
                    if server_name not in servers:
                        servers[server_name] = {}
                    servers[server_name]["geolocation"] = (lat, lon)
                except ValueError:
                    # Skip if not valid floats
                    continue

            # Handle config for server host
            # Example line: 'active.node1=10.10.1.3:2000'
            parts = line.split("=")
            if len(parts) == 2 and len(parts[0].split(".")) == 2:
                key_parts = parts[0].split(".")
                server_name = key_parts[1]

                # get the host, assuming it as IP address
                host_parts = parts[1].split(":")
                host = host_parts[0]
                
                if server_name not in servers:
                    servers[server_name] = {}
                servers[server_name]["host"] = host
    
    return servers

def get_latency_slowdown_from_property_file(filename: str) -> float:
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            line = line.partition('#')[0]

            # Skip empty lines or commented lines if any
            if not line or line.startswith("#"):
                continue
            
            if line.startswith("XDN_EVAL_LATENCY_SLOWDOWN_FACTOR"):
                parts = line.split('=')
                if len(parts) < 2:
                    continue
                parts = parts[1].strip().split()
                slowdown_str = parts[0]
                slowdown = float(slowdown_str)
                return slowdown
    return 1.0

def get_estimated_latency(distance_km, slowdown_factor):
    """
    Estimate the one-way latency (in milliseconds) for a signal traveling 
    'distance_km' kilometers, taking into account the speed of light and 
    a 'slowdown_factor'.

    :param distance_km:       The distance in kilometers (float).
    :param slowdown_factor:   A factor (0 < slowdown_factor <= 1) indicating 
                              how much slower than the speed of light 
                              the signal travels.
    :return:                  Estimated latency in milliseconds (float).

    Example:
        >>> # Suppose a distance of 1000 km, with a slowdown factor of 0.1
        >>> # This means the signal travels at 10% the speed of light
        >>> latency_ms = estimate_latency(1000, 0.8)
        >>> print(latency_ms, "ms")
    """

    # Validate slowdown factor
    if not (0 < slowdown_factor <= 1):
        raise ValueError("Slowdown factor must be between 0 and 1 (exclusive of 0, inclusive of 1).")

    # Speed of light in km/s
    speed_of_light_km_s = 299792.458

    # Time in seconds = distance / (speed of light * slowdown factor)
    time_seconds = distance_km / (speed_of_light_km_s * slowdown_factor)

    # Convert to milliseconds
    time_milliseconds = time_seconds * 1000

    return time_milliseconds

def inject_server_latency(servers, net_device, slowdown, 
                          device_exception_map=None, enable_minimum_latency=False):
    server_names = list(servers.keys())

    # reseting the injected latency
    print("\nResetting the emulated latency:")
    reset_latency_injection(servers, net_device, device_exception_map=device_exception_map)

    def inject_worker(src_name, dst_name):
        lat1, lon1 = servers[src_name]["geolocation"]
        lat2, lon2 = servers[dst_name]["geolocation"]
        distance_km = haversine_distance(lat1, lon1, lat2, lon2)
        expected_latency_ms = get_estimated_latency(distance_km, slowdown)  # one-way delay
        expected_rtt_latency_ms = expected_latency_ms * 2.0                 # double for rtt
        if enable_minimum_latency:
            expected_latency_ms = max(expected_rtt_latency_ms, MINIMUM_INTER_SERVER_RTT_LATENCY_MS)

        src_network = servers[src_name]["host"]
        dst_network = servers[dst_name]["host"]

        # observe the current ping latency to get the offset
        command = f"ssh {src_network} ping -c 3 {dst_network} | tail -1 | awk '{{print $4}}' | cut -d '/' -f 2"
        proc_result = subprocess.run(command, shell=True, capture_output=True, text=True)
        offset_latency_ms = float(proc_result.stdout.strip())

        # calculate the injected latency
        injected_latency_ms = expected_latency_ms - offset_latency_ms
        injected_latency_ms = max(injected_latency_ms, 0.0)

        # handle device exception
        target_net_device = net_device
        if device_exception_map != None and src_network in device_exception_map:
            target_net_device = device_exception_map[src_network]

        command = f"ssh {src_network} sudo tcset {target_net_device} --delay {injected_latency_ms}ms --network {dst_network} --add"
        print(f">>> {src_network} <-> {dst_network}: exp={expected_latency_ms:.3f}ms off={offset_latency_ms:.3f}ms dly={injected_latency_ms:.3f}ms\n" + ">> " + command)
        proc_result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if proc_result.returncode != 0:
            print("ERROR :(")
            
        return
    
    # preparing all threads
    per_machine_threads = {}
    total_num_workers = 0
    for i in range(len(server_names)):
        per_machine_threads[i] = []
        for j in range(i + 1, len(server_names)):
            s1 = server_names[i]
            s2 = server_names[j]
            t = threading.Thread(target=inject_worker, args=(s1, s2))
            per_machine_threads[i].append(t)
            total_num_workers += 1
    
    # inter-machine concurrency
    # we can only run the command in parallel from different machine
    while total_num_workers > 0:
        threads = []
        for i in range(len(server_names)):
            if len(per_machine_threads[i]) > 0:
                t = per_machine_threads[i].pop(0)
                threads.append(t)
        total_num_workers -= len(threads)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    return

def reset_latency_injection(servers, net_device, device_exception_map=None):
    server_names = list(servers.keys())

    # prepare threads for reseting latency
    def reset_thread(server_host, net_dev_if_name):
        command = f'ssh {server_host} sudo tcdel {net_dev_if_name} --all'
        print(">> " + command)
        proc_result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if proc_result.returncode != 0:
            print(f"ERROR, retcode={proc_result.returncode}")
        return
    
    # reseting the injected latency, in multiple threads
    threads = []
    for i in range(len(server_names)):
        s1 = server_names[i]
        host = servers[s1]["host"]

        # handle device exception
        target_net_device = net_device
        if device_exception_map != None and host in device_exception_map:
            target_net_device = device_exception_map[host]

        t = threading.Thread(target=reset_thread, args=(host, target_net_device))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    return

def printout_injected_latency(servers, slowdown, enable_minimum_latency=False):
    # Get a list of server names so we can pair them
    server_names = list(servers.keys())

    # Printout all server names and host
    print("All the registered servers:")
    for name in server_names:
        print(" >> " + name + " " + servers[name]["host"])
    print("Slowdown factor: " + str(slowdown))

    # Calculate distances and latency between all unique pairs
    print("\nDistance and latency between server pairs:")
    for i in range(len(server_names)):
        for j in range(i + 1, len(server_names)):
            s1 = server_names[i]
            s2 = server_names[j]
            h1 = servers[s1]["host"]
            h2 = servers[s2]["host"]
            lat1, lon1 = servers[s1]["geolocation"]
            lat2, lon2 = servers[s2]["geolocation"]
            distance_km = haversine_distance(lat1, lon1, lat2, lon2)
            latency_ms = get_estimated_latency(distance_km, slowdown)
            if enable_minimum_latency:
                latency_ms = max(latency_ms, MINIMUM_INTER_SERVER_LATENCY_MS)
            print(f" >> {s1}/{h1} <-> {s2}/{h2}:\t{distance_km:8.2f} km \t ({latency_ms} ms)")

def verify_injected_latency(servers, slowdown, enable_minimum_latency=False):
    """
    Returns True if the observed latency is within 0.5 ms of the expectation,
    otherwise returns False.
    """
    printout_injected_latency(servers, slowdown)
    server_names = list(servers.keys())
    expectation_map = {}

    def verify_worker(i, j):
        s1 = server_names[i]
        s2 = server_names[j]
        h1 = servers[s1]["host"]
        h2 = servers[s2]["host"]
        lat1, lon1 = servers[s1]["geolocation"]
        lat2, lon2 = servers[s2]["geolocation"]
        distance_km = haversine_distance(lat1, lon1, lat2, lon2)
        latency_ms = get_estimated_latency(distance_km, slowdown)
        if enable_minimum_latency:
            latency_ms = max(latency_ms, MINIMUM_INTER_SERVER_LATENCY_MS)

        max_trials = 5

        # from h1
        command = f"ssh {h1} ping -c 3 {h2} | tail -1 | awk '{{print $4}}' | cut -d '/' -f 2"
        latency_h1_ms = 0
        for curr_trial in range(max_trials):
            try:
                proc_result = subprocess.run(command, shell=True, capture_output=True, text=True)
                latency_h1_ms = float(proc_result.stdout.strip())
                break
            except:
                if curr_trial == max_trials-1:
                    raise Exception(f"Failed to observe latency after {max_trials} attempts.")
                else:
                    time.sleep(10 ** (curr_trial))
                    continue

        # from h2
        command = f"ssh {h2} ping -c 3 {h1} | tail -1 | awk '{{print $4}}' | cut -d '/' -f 2"
        latency_h2_ms = 0
        for curr_trial in range(max_trials):
            try:
                proc_result = subprocess.run(command, shell=True, capture_output=True, text=True)
                latency_h2_ms = float(proc_result.stdout.strip())
                break
            except:
                if curr_trial == max_trials-1:
                    raise Exception(f"Failed to observe latency after {max_trials} attempts.")
                else:
                    time.sleep(10 ** (curr_trial))
                    continue

        avg_lat_ms = (float(latency_h1_ms) + float(latency_h2_ms)) / 2
        diff_lat_ms = avg_lat_ms - latency_ms
        
        print(f" >> {s1}/{h1} <-> {s2}/{h2}:\t({latency_h1_ms:6.3f} ms) & ({latency_h2_ms:6.3f} ms)\t vs. ({latency_ms:6.3f} ms)\t diff={diff_lat_ms:.2f}ms")
        
        expectation_map[f"{i}:{j}"] = diff_lat_ms
        return
    
    print("\nObserved latency between server pairs:")
    threads = []
    for i in range(len(server_names)):
        for j in range(i + 1, len(server_names)):
            t = threading.Thread(target=verify_worker, args=(i, j))
            threads.append(t)
            t.start()

    for t in threads:
        t.join()

    for pair, lat_diff in expectation_map.items():
        if abs(lat_diff) > 0.5:
            print(f"ERROR: unexpected latency pair {pair} {lat_diff}ms")
            return False
    return True


if __name__ == "__main__":
    import argparse

    valid_modes = {"execute", "dry", "reset", "verify"}

    parser = argparse.ArgumentParser(description="Injecting inter-server latency with tc")
    parser.add_argument("-m", "--mode", type=str, default="execute", help=f"Supported mode: {valid_modes}")
    parser.add_argument("-d", "--device", type=str, default=DEFAULT_NET_DEV_INTERFACE_NAME, help="Network device interface name (e.g: eno1)")
    parser.add_argument("-c", "--config", type=str, default=DEFAULT_CONFIG_FILE, help="Gigapaxos config file, containing servers data")
    parser.add_argument("-l", "--enable-minimum-lat", type=bool, default=False, help="Enable minimum latency, suitable for availability zone with same geolocation.")
    parser.add_argument("-e", "--device-exceptions", type=str, default="", help="Comma separated exception for device name, e.g., '10.10.1.1/ens2f1np1,10.10.1.3/ens2f0np0'. The usage is when some machines in the cluster has different device name.")

    args = parser.parse_args()

    # Validate mode
    if args.mode not in valid_modes:
        print(f"Invalid mode of '{args.mode}', valid options are {valid_modes}.")
        exit(-1)

    # Validate device
    if args.device == "":
        print("Invalid network device interface name.")
        exit(-1)

    # Validate device exceptions
    dev_exception_map = None
    if args.device_exceptions != "":
        dev_exception_map = {}
        temp_pairs = args.device_exceptions.split(",")
        if len(temp_pairs) == 0:
            print("Invalid network device exception. Example: '10.10.1.1/ens2f1np1,10.10.1.3/ens2f0np0'")
            exit(-1)
        for pair in temp_pairs:
            parts = pair.split("/")
            if len(parts) != 2:
                print("Invalid network device exception. Example: '10.10.1.1/ens2f1np1,10.10.1.3/ens2f0np0'")
                exit(-1)
            address = parts[0]
            dev_name = parts[1]
            if dev_name == args.device:
                print(f"Cannot use the default device {args.device} in the exception list")
                exit(-1)
            dev_exception_map[address] = dev_name

    # Read all servers' geolocations and IP
    servers = read_servers_from_property_file(args.config)
    slowdown = get_latency_slowdown_from_property_file(args.config)

    if args.mode == "execute":
        inject_server_latency(servers, args.device, slowdown, 
                              device_exception_map=dev_exception_map, 
                              enable_minimum_latency=args.enable_minimum_lat)
    elif args.mode == "reset":
        reset_latency_injection(servers, args.device, 
                                device_exception_map=dev_exception_map)
    elif args.mode == "dry":
        printout_injected_latency(servers, slowdown, 
                                  enable_minimum_latency=args.enable_minimum_lat)
    elif args.mode == "verify":
        is_expected = verify_injected_latency(servers, slowdown, 
                                              enable_minimum_latency=args.enable_minimum_lat)
        if not is_expected:
            print("Found ping latency larger than 0.5ms of the expected latency :(")
            exit(-1)
