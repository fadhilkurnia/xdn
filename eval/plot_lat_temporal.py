import os
import re
import csv
import statistics
import numpy as np
import matplotlib.pyplot as plt
from replica_group_picker import get_client_locations

latency_result_dir_path='./results_lat_temporal'
latency_result_expectation_file = 'results/temporal_latencies_expected.csv'
latency_result_file = 'results/temporal_latencies_observed.csv'
population_data_file = "location_distributions/client_us_metro_population.csv"

is_use_world_population = True

if is_use_world_population:
    population_data_file = "location_distributions/client_world_metro_population.csv"

city_locations = get_client_locations(population_data_file)

# sort city by latitude and longitude, from east to west.
# assign metro ID consistent across different approaches
sorted_cities = sorted(city_locations, key=lambda d: (d['Longitude'], d['Latitude']), reverse=False)
for i in range(len(sorted_cities)):
    city_name = sorted_cities[i]['City']
    sorted_cities[i]['MetroID'] = i + 1
sorted_city_names = []
for city in sorted_cities:
    sorted_city_names.append(city['City'])
city_by_name = {}
for city in sorted_cities:
    city_by_name[city['City']] = city

def generate_csv_result_file(dir_path, latency_result_file):
    raw_results = []
    latency_files = os.listdir(latency_result_dir_path)
    for filename in latency_files:
        parts = filename.split('_')
        if len(parts) != 5:
            print(f"WARNING: ignoring file {filename} because it has incorrect filename format.")
            continue
        curr_approach = parts[2]
        curr_timestamp = parts[3][1:]
        curr_raw_city_name = parts[4][1:-4]
        curr_latencies = []
        with open(latency_result_dir_path + '/' + filename, 'r') as tsvfile:
            tsv_reader = csv.DictReader(tsvfile, delimiter='\t')
            for row in tsv_reader:
                curr_latencies.append(float(row["ttime"]))
        curr_avg_latency = statistics.mean(curr_latencies)
        curr_med_latency = statistics.median(curr_latencies)
        curr_min_latency = min(curr_latencies)
        curr_max_latency = max(curr_latencies)
        curr_p90_latency = np.percentile(curr_latencies, 90)
        curr_p95_latency = np.percentile(curr_latencies, 95)
        curr_p99_latency = np.percentile(curr_latencies, 99)
        
        # Convert PascalCase into space separeted words.
        # Insert a space before each uppercase letter (except at the start)
        curr_city_name = re.sub(r'(?<!^)(?=[A-Z])', ' ', curr_raw_city_name)
        if curr_city_name == "Dares Salaam":
            curr_city_name = "Dar es Salaam"
        if curr_city_name == "Washington D C":
            curr_city_name = "Washington DC"
        if curr_city_name == "Riode Janeiro":
            curr_city_name = "Rio de Janeiro"

        data = {'Timestamp': curr_timestamp, 
                'Approach': curr_approach, 
                'Latitude': city_by_name[curr_city_name]['Latitude'],
                'Longitude': city_by_name[curr_city_name]['Longitude'],
                'CityName': curr_city_name,
                'NumClient': len(curr_latencies),
                'AvgLatMilisecond': curr_avg_latency,
                'MinLatMilisecond': curr_min_latency,
                'MaxLatMilisecond': curr_max_latency,
                'P50LatMilisecond': curr_med_latency,
                'P90LatMilisecond': curr_p90_latency,
                'P95LatMilisecond': curr_p95_latency,
                'P99LatMilisecond': curr_p99_latency
        }
        raw_results.append(data)

    with open(latency_result_file, 'w') as result_file:
        result_file.write('timestamp_utc,approach,city_lat,city_lon,city_name,client_count,avg_lat_ms,min_lat_ms,max_lat_ms,p50_lat_ms,p90_lat_ms,p95_lat_ms,p99_lat_ms\n')
        for data in raw_results:
            result_file.write(f"{data['Timestamp']},{data['Approach']},{data['Latitude']},{data['Longitude']},{data['CityName']},{data['NumClient']},{data['AvgLatMilisecond']},{data['MinLatMilisecond']},{data['MaxLatMilisecond']},{data['P50LatMilisecond']},{data['P90LatMilisecond']},{data['P95LatMilisecond']},{data['P99LatMilisecond']}\n")
        result_file.close()

    return

def generate_graph_from_csv_result_file(csv_file_path, generated_pdf_filename):
    # read csv file into a list of result
    raw_result = []
    with open(csv_file_path, 'r') as result_file:
        reader = csv.DictReader(result_file)
        for row in reader:
            data = {'Timestamp': row['timestamp_utc'],
                    'Approach': row['approach'],
                    'Latitude': row['city_lat'],
                    'Longitude': row['city_lon'],
                    'CityName': row['city_name'],
                    'NumClient': int(row['client_count']),
                    'AvgLatMilisecond': float(row['avg_lat_ms']) if "avg_lat_ms" in row else 0.0,
                    'MinLatMilisecond': float(row['min_lat_ms']) if "min_lat_ms" in row else 0.0,
                    'MaxLatMilisecond': float(row['max_lat_ms']) if "max_lat_ms" in row else 0.0,
                    'P50LatMilisecond': float(row['p50_lat_ms']) if "p50_lat_ms" in row else 0.0,
                    'P90LatMilisecond': float(row['p90_lat_ms']) if "p90_lat_ms" in row else 0.0,
                    'P95LatMilisecond': float(row['p95_lat_ms']) if "p95_lat_ms" in row else 0.0,
                    'P99LatMilisecond': float(row['p99_lat_ms']) if "p99_lat_ms" in row else 0.0
                }
            raw_result.append(data)
    
    approach_set = set()
    for data in raw_result:
        approach_set.add(data['Approach'])
    approaches = list(approach_set)

    latencies_xdn = []
    latencies_xdnnr = []
    latencies_gd = []
    latencies_cd = []
    latencies_ed = []

    for approach in approaches:
        # Collect the average latencies per city.
        # We consider the client count so that later we can average the overall
        # latencies per city
        per_city_latencies = {}
        for data in raw_result:
            if data['Approach'] != approach:
                continue
            city_name = data['CityName']
            if city_name not in per_city_latencies:
                per_city_latencies[city_name] = []
            for i in range(data['NumClient']):  # adds proportional to the client count
                per_city_latencies[city_name].append(data['AvgLatMilisecond'])

        # aggregate the average
        per_city_avg_lat_ms = {}
        for city_name, latencies in per_city_latencies.items():
            if len(latencies) > 0:
                avg_lat_ms = statistics.mean(latencies)
                per_city_avg_lat_ms[city_name] = avg_lat_ms

        # store the average latency per city data, per approach, sorted by the cities
        for i in range(len(sorted_cities)):
            city_name = sorted_cities[i]['City']
            curr_city_avg_lat_ms = 0
            if city_name in per_city_avg_lat_ms:
                curr_city_avg_lat_ms = per_city_avg_lat_ms[city_name]
            
            if approach == 'XDN':
                latencies_xdn.append(curr_city_avg_lat_ms)
            elif approach == 'XDNNR':
                latencies_xdnnr.append(curr_city_avg_lat_ms)
            elif approach == 'CD':
                latencies_cd.append(curr_city_avg_lat_ms)
            elif approach == 'ED':
                latencies_ed.append(curr_city_avg_lat_ms)
            elif approach == 'GD':
                latencies_gd.append(curr_city_avg_lat_ms)
            else:
                raise Exception(f'Unsupported approach {approach}')
            
    # display the aggregated latency
    print(f">> MEASUREMENT RESULTS: {generated_pdf_filename}")
    blank = ""
    approach_text = ""
    for approach in approaches:
        approach_text += approach + "\t"
    print(f"{blank:20}\t{approach_text}")
    for i, city_name in enumerate(sorted_city_names):
        row_text = f"{city_name:20}\t"
        for approach in approaches:
            avg_lat_ms = 0.0
            if approach == 'XDN':
                avg_lat_ms = latencies_xdn[i]
            elif approach == 'XDNNR':
                avg_lat_ms = latencies_xdnnr[i]
            elif approach == 'CD':
                avg_lat_ms = latencies_cd[i]
            elif approach == 'ED':
                avg_lat_ms = latencies_ed[i]
            elif approach == 'GD':
                avg_lat_ms = latencies_gd[i]
            else:
                raise Exception(f'Unsupported approach {approach}')
            pass
            row_text += f"{avg_lat_ms:5.2f}\t"
        print(row_text)

    print()
    print("Across city average:")
    print(f"{blank:20}\t{approach_text}")
    row_text = f"{blank:20}\t"
    for approach in approaches:
        avg_lat_ms = 0.0
        if approach == 'XDN':
            avg_lat_ms = statistics.mean(latencies_xdn)
        elif approach == 'XDNNR':
            avg_lat_ms = statistics.mean(latencies_xdnnr)
        elif approach == 'CD':
            avg_lat_ms = statistics.mean(latencies_cd)
        elif approach == 'ED':
            avg_lat_ms = statistics.mean(latencies_ed)
        elif approach == 'GD':
            avg_lat_ms = statistics.mean(latencies_gd)
        else:
            raise Exception(f'Unsupported approach {approach}')
        pass
        row_text += f"{avg_lat_ms:5.2f}\t"
    print(row_text)

    
    # plot the aggregated latencies
    num_metros = len(sorted_cities)
    print(">> num metros: ", num_metros)
    x = 1 + np.arange(num_metros)

    # Sorted latency by row in the figure
    latencies_1 = latencies_gd
    latencies_2 = latencies_cd
    latencies_3 = latencies_ed
    latencies_4 = latencies_xdn

    # Create figure + 4×1 subplots
    fig, axes = plt.subplots(4, 1, figsize=(12, 2))

    # List of data arrays for convenience
    all_latencies = [latencies_1, latencies_2, latencies_3, latencies_4]
    titles = ['Multi Region', 'Static (us-east-1)', 'Edge Datastore', 'XDN']
    colors = ['red', 'green', 'orange', 'gold']

    # Flatten axes so we can iterate easily
    axes_flat = axes.flat
    idxs = np.arange(4)

    # Get y-lim
    y_max = 0
    for lats in all_latencies:
        for lat in lats:
            if lat > y_max:
                y_max = lat
    y_top_lim = y_max * 1.05

    for idx, ax, latencies, title, color in zip(idxs, axes_flat, all_latencies, titles, colors):
        # Plot the bar chart
        ax.bar(x, latencies, color=color, alpha=0.4)

        if idx == 3:
            updated_sorted_city_names = []
            for i, name in enumerate(sorted_city_names):
                if i % 2 == 0:
                    updated_sorted_city_names.append(name)
                else:
                    updated_sorted_city_names.append("")
            ax.set_xticks(x, sorted_city_names, rotation=90, fontsize=7)
            ax.set_xlabel('Metro Area')
        else:
            ax.set_xticks([])
        
        # Remove left/right padding
        ax.set_xlim(left=0.5, right=len(sorted_cities)+0.5)
        ax.set_ylim(bottom=0, top=y_top_lim)
        
        # Titles and labels
        ax.text(1, y_top_lim*0.6, title, fontsize=10)
        ax.set_ylabel(' ')
        
        # Compute the average
        mean_val = np.mean(latencies)
        
        # Add a horizontal line at the average
        ax.axhline(y=mean_val, color='red', linestyle='--', linewidth=1)
        
        # Option A: Place the text in data coordinates near the horizontal line
        # Adjust 'va' and 'ha' to avoid overlapping the line if you prefer
        ax.text(
            100,                 # Place text near the right-most bar
            mean_val,           # Align with the average line vertically
            f"{mean_val:.2f} ms",
            ha='right',
            va='bottom',
            color='black',
            fontsize=8,
            weight="bold"
        )

    fig.text(0.065, 0.45, 'Avg. Latency (ms)', va='center', rotation='vertical')
    # plt.tight_layout()
    plt.savefig(generated_pdf_filename, format="pdf", bbox_inches="tight")

    return

# process data from `results_lat_temporal` directory and output 
# into `temporal_latencies_observed.csv`
# generate_csv_result_file(latency_result_dir_path, latency_result_file)

# Generate the graph
generate_graph_from_csv_result_file(latency_result_expectation_file, 'plots/latency_temporal_clients_expected_new.pdf')
generate_graph_from_csv_result_file(latency_result_file, 'plots/latency_temporal_clients_new.pdf')