#!/usr/bin/env python3

import requests
import time
import statistics

SERVICE_NAME="bookcatalog"
REQUEST_URL="http://10.10.1.3:2300/api/books"
REQUEST_PAYLOAD='{"title": "Distributed Systems", "author": "Tanenbaum"}'

# To move a replica group
# curl -v -X POST http://10.10.1.11:3300/api/v2/services/bookcatalog/placement -d '["nyc", "phl", "bos"]'

def measure_latency(url, num_requests=100, timeout=5):
    latencies = []
    for i in range(num_requests):
        start_time = time.perf_counter()
        try:
            # Send a POST request
            response = requests.post(REQUEST_URL, 
                                     timeout=timeout, 
                                     headers={"XDN": SERVICE_NAME}, 
                                     json=REQUEST_PAYLOAD)
            end_time = time.perf_counter()
            
            # Calculate the time taken for this request
            latencies.append(end_time - start_time)

        except requests.exceptions.Timeout:
            # If there's a timeout, ignore this request
            print(f"Request {i+1} timed out. Ignoring this request.")
        except requests.exceptions.RequestException as e:
            # Catch all other requests-related errors
            print(f"Request {i+1} failed: {e}. Ignoring this request.")

    # Check how many requests succeeded
    if len(latencies) == 0:
        print("All requests either timed out or failed.")
        return

    # Calculate statistics
    avg_latency = statistics.mean(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)

    print(f"Number of successful requests: {len(latencies)}")
    print(f"Average latency: {avg_latency:.4f} seconds")
    print(f"Minimum latency: {min_latency:.4f} seconds")
    print(f"Maximum latency: {max_latency:.4f} seconds")

    # Store the result in a csv file: latencies.csv

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Measure latency of deployed XDN service.")
    parser.add_argument("url", help="The URL to test.")
    parser.add_argument("-n", "--num-requests", type=int, default=100, help="Number of requests to perform.")
    parser.add_argument("-t", "--timeout", type=float, default=5, help="Request timeout in seconds.")
    args = parser.parse_args()

    measure_latency(args.url, args.num_requests, args.timeout)
