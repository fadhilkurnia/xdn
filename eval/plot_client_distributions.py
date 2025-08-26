import sys
import csv
import matplotlib.pyplot as plt
import numpy as np

def read_csv_data(csv_file):
    """
    Reads lat, lon, and population columns from a CSV file.
    Assumed columns:
       city, location (lat;lon), population
    Returns three lists: latitudes, longitudes, populations.
    """
    latitudes = []
    longitudes = []
    populations = []
    
    with open(csv_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["location"] == "NA;NA":
                continue
            lat_str, lon_str = row["location"].split(";")
            latitudes.append(float(lat_str))
            longitudes.append(float(lon_str))
            populations.append(float(row["population"]))
    
    return latitudes, longitudes, populations

def main():
    """
    Usage:
        python plot_cities.py cities1.csv [cities2.csv ...]

    This script creates a figure with:
      - One row of 3 subplots per CSV file:
        1) Scatter (lon vs lat, size~population)
        2) Horizontal bar chart of lat frequency
        3) Vertical bar chart of lon frequency
      - All rows share the same global lat/lon limits and frequency limits.
    """
    csv_files = sys.argv[1:]
    if not csv_files:
        print("Please provide at least one CSV file.")
        sys.exit(1)
    
    # --------------------------------------------
    # 1) FIRST PASS: COLLECT GLOBAL LIMITS
    #    - min/max lat/lon across all data
    #    - max frequency across all data
    # --------------------------------------------
    all_lats = []
    all_lons = []
    
    # We'll define how many bins for lat/lon hist
    bins_lat = 200
    bins_lon = 200
    
    # Keep track of the global maximum frequency
    global_lat_freq_max = 0
    global_lon_freq_max = 0
    
    # Pass 1: read each file, gather lat/lon/pops for global range, 
    # and do local histograms to track max frequency.
    for file in csv_files:
        lats, lons, pops = read_csv_data(file)
        
        # Add to the master list for lat/lon range
        all_lats.extend(lats)
        all_lons.extend(lons)
        
        # Compute local histograms for lat & lon
        lat_counts, _ = np.histogram(lats, bins=bins_lat)
        lon_counts, _ = np.histogram(lons, bins=bins_lon)
        
        # Update global max frequency
        max_lat_freq = max(lat_counts) if len(lat_counts) > 0 else 0
        max_lon_freq = max(lon_counts) if len(lon_counts) > 0 else 0
        
        if max_lat_freq > global_lat_freq_max:
            global_lat_freq_max = max_lat_freq
        if max_lon_freq > global_lon_freq_max:
            global_lon_freq_max = max_lon_freq
    
    # Now we have global lat/lon boundaries
    min_lat = min(all_lats)
    max_lat = max(all_lats)
    min_lon = -130 #min(all_lons)
    max_lon = -60 #max(all_lons)
    
    # --------------------------------------------
    # 2) SECOND PASS: CREATE PLOTS
    # --------------------------------------------
    fig, axes = plt.subplots(
        nrows=len(csv_files),
        ncols=3,
        figsize=(12, 0.5 * len(csv_files)),
        squeeze=False  # always returns 2D array for axes
    )
    
    for row_idx, csv_file in enumerate(csv_files):
        latitudes, longitudes, populations = read_csv_data(csv_file)
        
        # Unpack subplots for this row
        ax_scatter = axes[row_idx, 0]  # scatter plot
        ax_latfreq = axes[row_idx, 1]  # lat freq barh
        ax_lonfreq = axes[row_idx, 2]  # lon freq bar
        
        # ---------- A) Scatter Plot ----------
        population_sum = 0
        for p in populations:
            population_sum += p
        size_factor = 300.0
        marker_sizes = [p / population_sum * size_factor for p in populations]
        
        ax_scatter.scatter(
            longitudes,
            latitudes,
            s=marker_sizes,
            c='blue',
            alpha=0.6,
            edgecolors='k'
        )
        ax_scatter.set_xlabel("Longitude (°)")
        ax_scatter.set_ylabel("Latitude (°)")
        # ax_scatter.set_title(f"Scatter: {csv_file}")
        
        # Set global lat/lon bounds
        ax_scatter.set_xlim(min_lon, max_lon)
        ax_scatter.set_ylim(min_lat, max_lat)
        
        # ---------- B) Latitude Frequency (barh) ----------
        lat_counts, lat_bin_edges = np.histogram(latitudes, bins=bins_lat)
        lat_bin_centers = 0.5 * (lat_bin_edges[:-1] + lat_bin_edges[1:])
        lat_bin_widths = lat_bin_edges[1:] - lat_bin_edges[:-1]
        
        ax_latfreq.barh(
            lat_bin_centers,
            lat_counts,
            height=0.8 * lat_bin_widths,
            color='green',
            alpha=0.7,
            edgecolor='black'
        )
        ax_latfreq.set_xlabel("Frequency")
        ax_latfreq.set_ylabel("Latitude (°)")
        # ax_latfreq.set_title(f"Lat Freq: {csv_file}")
        
        # Use global lat bounds for the y-axis
        ax_latfreq.set_ylim(min_lat, max_lat)
        # Use global frequency bounds (0 -> global_lat_freq_max) for the x-axis
        ax_latfreq.set_xlim(0, global_lat_freq_max)
        
        # ---------- C) Longitude Frequency (bar) ----------
        lon_counts, lon_bin_edges = np.histogram(longitudes, bins=bins_lon)
        lon_bin_centers = 0.5 * (lon_bin_edges[:-1] + lon_bin_edges[1:])
        lon_bin_widths = lon_bin_edges[1:] - lon_bin_edges[:-1]
        
        ax_lonfreq.bar(
            lon_bin_centers,
            lon_counts,
            width=0.8 * lon_bin_widths,
            color='orange',
            alpha=0.7,
            edgecolor='black'
        )
        ax_lonfreq.set_xlabel("Longitude (°)")
        ax_lonfreq.set_ylabel("Frequency")
        # ax_lonfreq.set_title(f"Lon Freq: {csv_file}")
        
        # Use global lon bounds for the x-axis
        ax_lonfreq.set_xlim(min_lon, max_lon)
        # Use global frequency bounds (0 -> global_lon_freq_max) for the y-axis
        ax_lonfreq.set_ylim(0, global_lon_freq_max)
    
    # Tight layout to avoid overlap
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
