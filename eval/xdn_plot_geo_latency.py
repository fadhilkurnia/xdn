import os
import re
import csv
import statistics
import numpy as np

latency_result_dir_path = "/mydata/latency-results"
latency_files = os.listdir(latency_result_dir_path)

target_aggr_filename = "latency_geolocality.csv"
# approach  geolocality locality_name lat_avg_ms lat_var

# gather all the parameters
geolocalities = set()
approaches = set()
locality_names = set()
for filename in latency_files:
    parts = filename.split('_')
    if len(parts) < 7:
        continue
    assert len(parts) == 7, f"Invalid number of parts: {len(parts)}, {filename}"
    approaches.add(parts[0])
    
    geolocality = float(parts[2][1:])
    geolocalities.add(geolocality)
    
    locality_name = parts[4][1:]
    locality_names.add(locality_name)

print("Approaches       : ", approaches)
print("Geolocality      : ", geolocalities)
print("Locality Names   : ", locality_names)

with open(target_aggr_filename, 'w') as target_aggragate_file:
    target_aggragate_file.write("approach, geolocality, locality_name, lat_avg_ms, lat_var\n")
    approach_stats = {}
    for approach in approaches:
        approach_stats[approach] = {}
        for geolocality in geolocalities:
            approach_stats[approach][geolocality] = []
            for locality_name in locality_names:
                pattern = f"{approach}_latency_g{geolocality}_b[0-9]*_s{locality_name}_c[a-zA-Z]*_i[0-9]*.tsv"
                matching_files = [f for f in latency_files if re.search(pattern, f)]
                if len(matching_files) == 0:
                    continue
                latencies_ms = []
                for latency_filename in matching_files:
                    with open(latency_result_dir_path + '/' +latency_filename, 'r') as tsvfile:
                        tsv_reader = csv.DictReader(tsvfile, delimiter='\t')
                        for row in tsv_reader:
                            latencies_ms.append(float(row["ttime"]))
                avg_latency = statistics.mean(latencies_ms)
                med_latency = statistics.median(latencies_ms)
                var_latency = statistics.variance(latencies_ms)

                print(f">>> {approach:5} g={geolocality} city={locality_name:11}\t: avg_lat={avg_latency:.2f}ms\t variance={var_latency:.2f}")
                target_aggragate_file.write(f"{approach}, {geolocality}, {locality_name}, {avg_latency:.2f}, {var_latency:.2f}\n")
                approach_stats[approach][geolocality].append(avg_latency)
    
    target_aggragate_file.close()
    
    print()
    print()
    geolocalities = list(geolocalities)
    geolocalities.sort(reverse=True)
    approaches = list(approaches)
    approaches.sort(reverse=True)
    for geolocality in geolocalities:
        for approach in approaches:
            latencies = approach_stats[approach][geolocality]
            if len(latencies) == 0:
                continue
            latencies = np.array(latencies)
            avg_latency = statistics.mean(latencies)
            med_latency = statistics.median(latencies)
            var_latency = statistics.variance(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            p90_latency = np.percentile(latencies, 90)
            p95_latency = np.percentile(latencies, 95)
            p99_latency = np.percentile(latencies, 99)
            print(f'>> Approach={approach:5}\t g={geolocality}\t avg={avg_latency:6.2f}ms var={var_latency:8.2f} | min={min_latency:6.2f}ms max={max_latency:6.2f}ms | p50={med_latency:6.2f}ms p90={p90_latency:6.2f}ms p95={p95_latency:6.2f}ms p99={p99_latency:6.2f}ms')
        print()
