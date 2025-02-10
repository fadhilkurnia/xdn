import csv

def main(reference_file, src_file):
    # index city->airport code
    city_to_code = {}
    ref_file = open(reference_file, "r")
    ref_csv_file = csv.DictReader(ref_file)
    for row in ref_csv_file:
        city_name = row["City"]
        airport_code = row["Airport Code"]
        city_to_code[city_name] = airport_code

    dst_file = open("result.csv", "w")
    fields = ["Name","City","Country", "Latitude", "Longitude"]
    writer = csv.DictWriter(dst_file, fieldnames=fields)
    writer.writeheader()

    src_file = open(src_file, "r")
    src_file_reader = csv.DictReader(src_file)
    for row in src_file_reader:
        city_name = row["City"]
        country_name = row["Country"]
        latitude = row["Latitude"]
        longitude = row["Longitude"]
        airport_code = row["Name"]
        if airport_code == "-":
            if city_name in city_to_code and city_to_code[city_name] != "":
                airport_code = city_to_code[city_name]

        # write updated row
        writer.writerow({
            "Name": airport_code.lower(),
            "City": city_name,
            "Country": country_name,
            "Latitude": latitude,
            "Longitude": longitude,
        })
        

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Populate city airport name, given a csv file with city name.")
    parser.add_argument("ref_file", help="Reference csv file")
    parser.add_argument("src_file", help="Source csv file")
    args = parser.parse_args()

    # TODO: validate source file


    main(args.ref_file, args.src_file)