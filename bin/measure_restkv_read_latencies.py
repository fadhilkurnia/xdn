import requests
import time
import numpy as np

# Number of upsert operations to perform
num_operations = 10000

# URL for the read operation
target_key = "foo"
url = 'http://127.0.0.1:2301/kv/' + target_key

# Data for upsert operation
def generate_data(i):
    return {
        "key": "foo",
        "value": f"value-{i}"
    }

# Measure latency
latencies = []

for i in range(num_operations):
    # Generate key-value data for this request
    data = generate_data(i)

    # Start the timer
    start_time = time.time()

    # Perform the read operation
    headers = {'XDN': 'restkv'}
    response = requests.get(url, headers=headers)

    # End the timer
    end_time = time.time()

    # Check for successful response
    if response.status_code != 200 and response.status_code != 201:
        print(f"Failed request {i}: {response.status_code}, {response.text}")
        continue

    # Calculate the latency for this operation in milliseconds
    latency = (end_time - start_time) * 1000
    latencies.append(latency)

    # Optional: print progress
    if (i + 1) % 100 == 0:
        print(f"Completed {i + 1} operations")

# Convert latencies to a numpy array for easier calculation
latencies = np.array(latencies)

# Calculate average latency
average_latency = np.mean(latencies)

# Calculate percentile latencies
percentiles = [50, 90, 95, 99]
percentile_values = np.percentile(latencies, percentiles)

# Print results
print(f"Performed {num_operations} read operations")
print(f"Average latency per operation: {average_latency:.6f} ms")
print("Latency percentiles:")
for p, val in zip(percentiles, percentile_values):
    print(f"{p}th percentile: {val:.6f} ms")
