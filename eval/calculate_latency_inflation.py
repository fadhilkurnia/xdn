import csv
import statistics
import numpy as np
import matplotlib.pyplot as plt
from replica_group_picker import get_distance

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

ping_dataset_file = "location_distributions/avg_ping_dataset.csv"
expected_latencies = []
observed_latencies = []
errors = []
inflation_data = []
with open(ping_dataset_file, 'r') as file:
    reader = csv.DictReader(file)
    header = reader.fieldnames
    print("Header:", header)
    for row in reader:
        src_id = row['src_id']
        src_city = row['src_city']
        src_latitude = float(row['src_latitude'])
        src_longitude = float(row['src_longitude'])
        dst_id = row['dst_id']
        dst_city = row['dst_city']
        dst_latitude = float(row['dst_latitude'])
        dst_longitude = float(row['dst_longitude'])
        avg_ping_ms = float(row['avg_ping_ms'])

        # if src_id == dst_id:
        #     continue

        distance_m = get_distance((src_latitude, src_longitude), (dst_latitude, dst_longitude))
        distance_km = distance_m / 1000.0
        c_latency = get_estimated_latency(distance_km, 1)

        expected_ping_lat = c_latency*2
        observed_ping_lat =  avg_ping_ms

        if expected_ping_lat == 0:
            continue

        lat_inflation = observed_ping_lat / expected_ping_lat
        inflation_data.append(lat_inflation)
        expected_latencies.append(expected_ping_lat)
        observed_latencies.append(observed_ping_lat)
        errors = observed_ping_lat - expected_ping_lat
        

avg_inflation = statistics.mean(inflation_data)
med_inflation = statistics.median(inflation_data)
min_inflation = min(inflation_data)
max_inflation = max(inflation_data)
p10_inflation = np.percentile(inflation_data, 10)
p25_inflation = np.percentile(inflation_data, 25)
p75_inflation = np.percentile(inflation_data, 75)
p90_inflation = np.percentile(inflation_data, 90)
p95_inflation = np.percentile(inflation_data, 95)
p99_inflation = np.percentile(inflation_data, 99)

print(f"Avg inflation: {avg_inflation}")
print(f"Med inflation: {med_inflation}")
print(f"Min inflation: {min_inflation}")
print(f"Max inflation: {max_inflation}")
print("---")
print(f"p10_inflation: {p10_inflation}")
print(f"p25_inflation: {p25_inflation}")
print(f"p75_inflation: {p75_inflation}")
print(f"p90_inflation: {p90_inflation}")
print(f"p95_inflation: {p95_inflation}")
print(f"p99_inflation: {p99_inflation}")

x = np.array(expected_latencies)
y = np.array(observed_latencies)
x_mean = np.mean(x)
y_mean = np.mean(y)
m = np.sum((x-x_mean)*(y-y_mean)) / np.sum((x-x_mean)**2)
b = y_mean - m * x_mean
print(f"Slope: {m:.4f}")
print(f"Intercept: {b:.4f}")

y_pred = m * x + b
mse = np.mean((y - y_pred)**2)
# Calculate R^2
ss_total = np.sum((y - y_mean)**2)
ss_res = np.sum((y - y_pred)**2)
r2 = 1 - (ss_res/ss_total)
print(f"MSE: {mse:.4f}")
print(f"R²: {r2:.4f}")

plt.scatter(x, y, color='blue', marker='.', label='Actual data')
plt.plot(x, y_pred, color='red', label='Regression line')
plt.plot(x, x * avg_inflation, color='green', label='Avg inflation line')
plt.plot(x, x * med_inflation, color='orange', label='Med inflation line')
plt.xlabel("Expected ping latency via c (ms)")
plt.ylabel("Observed ping latency (ms)")
plt.title("Linear Regression (Manual OLS)")
plt.ylim(bottom=0)
plt.xlim(left=0)
plt.legend()
plt.show()