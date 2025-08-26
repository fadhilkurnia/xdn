import json

input_target='location_distributions/server_gcp_region.json'
output_target='location_distributions/server_gcp_region.csv'

with open(input_target, 'r') as file:

    output_file = open(output_target, "w")
    output_file.write('Name,City,Country,Latitude,Longitude\n')

    data = json.load(file)
    counter = 1
    for region_name in data:
        city_country_name = data[region_name]['name']
        raw = city_country_name.split(', ')
        city_name = raw[0]
        country_name = raw[0]
        if len(raw) >= 2:
            country_name = raw[-1]
        latitude = data[region_name]['latitude']
        longitude = data[region_name]['longitude']
        print(counter, region_name, city_name, country_name, latitude, longitude)
        output_file.write(f"{region_name},{city_name},{country_name},{latitude},{longitude}\n")
        counter += 1