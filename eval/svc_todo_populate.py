import time
import requests
import statistics

service_name = "todo"
num_tasks = 100
service_target_url = "http://127.0.0.1:8080/api/todo/tasks"

headers = {"Content-Type": "application/json", "XDN": service_name}
post_data = []
for i in range(num_tasks):
    post_data.append(f"{{\"item\":\"task-{i}\"}}")

print(f">> Warming up ...")
prev_cookie = None
for i in range(num_tasks):
    try:
        response = requests.post(service_target_url, headers=headers, 
                                    data=post_data[i],
                                    timeout=1, cookies=prev_cookie)
        prev_cookie = response.cookies
    except Exception as e:
        print(f"Exception: {e}")


print(f">> Running measurements ...")
prev_cookie = None
latencies = []
for i in range(1000):
    start_time = time.perf_counter()
    try:
        response = None
        # if is_read:
        # response = requests.get(service_target_url, headers=headers, 
        #                         timeout=3, cookies=prev_cookie)
        # else:
        response = requests.post(service_target_url, headers=headers, 
                                    data=post_data[i % num_tasks],
                                    timeout=1, cookies=prev_cookie)
        prev_cookie = response.cookies
    except Exception as e:
        print(f"Exception: {e}")
    end_time = time.perf_counter()
    latency = end_time - start_time
    latency_ms = latency * 1_000.0
    latencies.append(latency_ms)

# gather the data
avg_lat_ms = statistics.mean(latencies)
med_lat_ms = statistics.median(latencies)
stdev_lat_ms = statistics.stdev(latencies)
print(f">> Result: avg_lat={avg_lat_ms:.2f}ms p50_lat={med_lat_ms}ms stddev={stdev_lat_ms}")
print()