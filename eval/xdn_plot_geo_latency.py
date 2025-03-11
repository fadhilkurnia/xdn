import os
import re
import csv
import statistics

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
    target_aggragate_file.write("approach, geolocality, locality_name, lat_avg_ms, lat_var")
    approach_stats = {}
    for approach in approaches:
        for geolocality in geolocalities:
            approach_stats[geolocality] = []
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

                print(f">>> {approach} g={geolocality} city={locality_name}\t: avg_lat={avg_latency:.2f}ms\t variance={var_latency:.2f}")
                target_aggragate_file.write(f"{approach}, {geolocality}, {locality_name}, {avg_latency:.2f}, {var_latency:.2f}")
                approach_stats[geolocality].append(avg_latency)
    
    target_aggragate_file.close()
    
    print()
    for approach in approaches:
        for geolocality in geolocalities:
            avg_latency = statistics.mean(approach_stats[approach][geolocality])
            print(f">>> {approach} g={geolocality} avg_lat={avg_latency:.2f}ms")
