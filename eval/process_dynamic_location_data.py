import csv
import os

reference_csv_filenames = [
    "location_distributions/client_fed_gov_irs.csv",
    "location_distributions/client_fed_gov_nih.csv",
    "location_distributions/client_fed_gov_usps.csv",
    "location_distributions/client_fed_gov_weather.csv",
    "location_distributions/client_us_population_ne.csv",
    "location_distributions/client_us_population_nyc.csv"   
]

working_dir = "location_distributions/dynamic_demand_location"
result_dir = "location_distributions/dynamic_demand_location_updated"

city_to_loc = {}
for filename in reference_csv_filenames:
    with open(filename, mode='r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["location"] == "NA;NA":
                continue
            city_name_str = row["city"]
            location_str = row["location"]
            city_to_loc[city_name_str] = location_str

os.makedirs(result_dir, exist_ok=True)

working_files = os.listdir(working_dir)
fieldnames = ["city","location","population"]
for filename in working_files:
    result_csv_file = result_dir + '/' + filename.split('.')[0] + "_updated.csv"

    filename = working_dir + '/' + filename
    print(f">> working on {filename}")

    with open(result_csv_file, mode='w') as wf:
        writer = csv.DictWriter(wf, fieldnames=fieldnames)
        writer.writeheader()
        
        with open(filename, mode='r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                city_name_str = row["city"]
                population_str = row["Active Users"]
                location_str = "NA;NA"
                if city_name_str in city_to_loc:
                    location_str = city_to_loc[city_name_str]
                writer.writerow({'city': city_name_str, 'location': location_str, 'population': population_str})
        

            