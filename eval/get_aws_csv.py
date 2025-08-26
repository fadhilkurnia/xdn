import json

input_target='location_distributions/server_aws_region.json'
output_target='location_distributions/server_aws_region_new.csv'

with open(input_target, 'r') as file:

    output_file = open(output_target, "w")
    output_file.write('Name,City,Country,Latitude,Longitude\n')

    data = json.load(file)
    counter = 1
    for region in data:
        region_name = region['code']
        city_name = region['city']
        country_name = region['country']
        latitude = region['lat']
        longitude = region['long']
        print(counter, region_name, city_name, country_name, latitude, longitude)
        # output_file.write(f"{region_name},{city_name},{country_name},{latitude},{longitude}\n")
        counter += 1