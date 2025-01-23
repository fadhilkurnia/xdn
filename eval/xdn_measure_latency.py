#!/usr/bin/env python3

import csv
import re
import requests
import statistics
import sys
import time
import math
from typing import List

SERVICE_NAME="bookcatalog"
REQUEST_PATH="/api/books"
REQUEST_PAYLOAD='{"title": "Distributed Systems", "author": "Tanenbaum"}'
PROXY_ENDPOINT="http://0.0.0.0:8080"
CLIENT_LOCATION="42.8864;-78.8784" # Buffalo, US

# To move a replica group
# curl -v -X POST http://10.10.1.11:3300/api/v2/services/bookcatalog/placement -d '["nyc", "phl", "bos"]'

# Example usage:
# python3 xdn_measure_latency.py bookcatalog --control-plane=10.10.1.11 --config-file=../conf/gigapaxos.cloudlab-virtual.properties --location=us_population_ne.csv --num-clients=1000 --proxy=0.0.0.0:8080

def main(service_name, config_file, control_plane, proxy, num_clients, repetition, timeout, location_file):
    # Validates that service name is not null or empty
    if service_name == None or service_name == "":
        print("Invalid service name! it cannot be empty.")
        sys.exit(1)

    # Checks replicas' address by contacting the control plane
    replica_addresses = get_replicas_address(service_name, control_plane)
    if len(replica_addresses) == 0:
        print(f"Failed to find replicas address for {service_name}")
        sys.exit(1)

    measure_latency_geo(service_name, replica_addresses, config_file, location_file, num_clients, repetition, timeout, proxy)
    return

def get_replicas_address(service_name, control_plane):
    control_plane_endpoint = f"http://{control_plane}:3300/?type=REQ_ACTIVES&name={service_name}"
    response = requests.get(control_plane_endpoint)
    if response.status_code != 200:
        return []
    
    replicas_metadata = response.json()
    if "ACTIVE_REPLICAS" not in replicas_metadata:
        return []
    
    # parse the response
    # assuming the format is "/10.10.1.1:2000"
    replicas = replicas_metadata["ACTIVE_REPLICAS"]
    for i, r in enumerate(replicas):
        replicas[i] = r[1:]
        replicas[i] = replicas[i].split(':')[0]

    return replicas

def measure_latency_geo(service_name, replica_addresses, config_file, location_file, num_clients, repetition, timeout, proxy):
    # validates address and config file
    if len(replica_addresses) == 0:
        return
    if config_file is None or config_file == "":
        print("XDN config file, containing the server geolocation, is required for geodistributed latency measurement")
        sys.exit(1)

    # get server names and locations, given the address and Gigapaxos/XDN config file
    servers = get_replica_location(replica_addresses, config_file)
    if len(servers) != len(replica_addresses):
        print(f"Failed to get replica locations from config file '{config_file}'")
        sys.exit(1)

    # prepare clients and assign to the closes replica
    city_clients = create_geo_clients(location_file, num_clients)
    prepared_clients = assign_client_replica(city_clients, servers)
    proxy_address = None
    if proxy is not None and proxy != "":
        proxy_address = { "http": f"http://{proxy}" }

    # do the actual measurements
    latencies = []
    for client in prepared_clients:
        for i in range(client["count"]):
            for j in range(repetition):
                target_server = client["target_replica"]
                latitude = client["latitude"]
                longitude = client["longitude"]
                client_location = f"{latitude};{longitude}"

                try:
                    start_time = time.perf_counter()
                    # Send a POST request
                    response = requests.post(f"http://{target_server}:2300{REQUEST_PATH}", 
                                            timeout=timeout,
                                            proxies=proxy_address,
                                            headers={
                                                "XDN": service_name, 
                                                "X-Client-Location": client_location},
                                            json=REQUEST_PAYLOAD)
                    end_time = time.perf_counter()
                    
                    # Calculate the time taken for this request
                    latencies.append(end_time - start_time)

                except requests.exceptions.Timeout:
                    # If there's a timeout, ignore this request
                    print(f"Client-{i}: Request {j+1} timed out. Ignoring this request.")
                except requests.exceptions.RequestException as e:
                    # Catch all other requests-related errors
                    print(f"Client-{i}: Request {j+1} failed: {e}. Ignoring this request.")

    # Check how many requests succeeded
    if len(latencies) == 0:
        print("All requests either timed out or failed.")
        return

    # Calculate statistics
    avg_latency = statistics.mean(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)

    print(f"Number of successful requests: {len(latencies)}")
    print(f"Average latency: {avg_latency*1_000:.4f} ms")
    print(f"Minimum latency: {min_latency*1_000:.4f} ms")
    print(f"Maximum latency: {max_latency*1_000:.4f} ms")

    # TODO: Store the result in a csv file: latencies.csv

    return

def get_replica_location(replica_addresses: List[str], config_file: str):
    file = open(config_file, "r")

    # maps server by its address
    servers = {}
    for address in replica_addresses:
        servers[address] = { "name": None, "latitude": None, "longitude": None }

    # find server name by address
    for line in file:
        for address in replica_addresses:
            if servers[address]["name"] is not None:
                continue
            if re.match(f"^active.[a-zA-Z0-9\-_]+={address}", line):
                key = line.split('=')[0]
                name = key.split('.')[1]
                servers[address]["name"] = name

    # find server location by name
    file.seek(0)
    for line in file:
        for address in replica_addresses:
            if servers[address]["latitude"] is not None:
                continue
            name = servers[address]["name"]
            if re.match(f"^active.{name}.geolocation=", line):
                geolocation = line.split('=')[1]
                parts = geolocation.strip().strip('"').split(",")
                latitude = float(parts[0].strip())
                longitude = float(parts[1].strip())
                servers[address]["latitude"] = latitude
                servers[address]["longitude"] = longitude

    file.close()
    return servers

def create_geo_clients(location_file, total_client_count):
    # Expected CSV column: "city", "population", "location", assuming location is in "latitude;longitude" format.
    # Output: [{ "name": "NYC", "count": 100, "latitude": 10.1, "longitude": 10.5 }, ...]

    if location_file is None or location_file == "":
        return [{"name": "bench-client", "count": total_client_count, "latitude": None, "longitude": None}]
    
    city_clients = []
    sum_population = 0
    with open(location_file, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            parts = row["location"].split(";")
            population = int(row["population"])
            latitude = float(parts[0].strip())
            longitude = float(parts[1].strip())
            city_clients.append({"name": row["city"],
                                 "population": row["population"],
                                 "latitude": latitude,
                                 "longitude": longitude})
            sum_population += population

    # populate client count for each client
    sum_client_count = 0
    for client in city_clients:
        client_count = float(client["population"]) / float(sum_population) * total_client_count
        client_count = int(round(client_count))
        if sum_client_count + client_count > total_client_count:
            client_count = total_client_count - sum_client_count
        
        client["count"] = client_count
        sum_client_count += client_count

    return city_clients

def assign_client_replica(city_clients, servers):
    for client in city_clients:
        min_distance = float("inf")
        closest_server = list(servers)[0]
        for server in servers:
            distance = haversine_distance(client["latitude"],
                                          client["longitude"],
                                          servers[server]["latitude"],
                                          servers[server]["longitude"])
            if distance < min_distance:
                min_distance = distance
                closest_server = server
        client["target_replica"] = closest_server

    return city_clients

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

def measure_latency_fastest():
    # TODO: measure latency by probing the closest replica
    return

def measure_latency_random():
    # TODO: measure latency by considering the client location distribution
    pass

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Measure latency of a deployed XDN service.")
    parser.add_argument("service_name", help="The service name to test.")
    parser.add_argument("-f", "--config-file", type=str, default="", help="XDN config file, containing server locations.")
    parser.add_argument("-c", "--control-plane", type=str, default="localhost", help="XDN control plane host address.")
    parser.add_argument("-p", "--proxy", type=str, default="", help="Host address of custom proxy for client latency injection.")
    parser.add_argument("-n", "--num-clients", type=int, default=1000, help="Number of clients.")
    parser.add_argument("-r", "--repetition", type=int, default=5, help="Request repetition for each client.")
    parser.add_argument("-t", "--timeout", type=float, default=5, help="Request timeout in seconds.")
    parser.add_argument("-l", "--location", type=str, default="", help="Path to location distribution csv file.")
    args = parser.parse_args()

    main(args.service_name, 
         args.config_file, 
         args.control_plane, 
         args.proxy, 
         args.num_clients, 
         args.repetition, 
         args.timeout, 
         args.location)

    # measure_latency(args.service_name, args.num_requests, args.timeout)
