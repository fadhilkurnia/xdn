import csv
import time
from geopy.geocoders import Nominatim

def main(src_csv_file, dst_csv_file):
    geolocator = Nominatim(user_agent="city-locator")
    with open(src_csv_file, "r", newline="", encoding="utf-8") as infile, \
        open(dst_csv_file, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)  # expects headers: city,population
        fieldnames = ["city", "location", "population"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            city_name = row["city"]
            population = row["population"]

            # Attempt to geocode
            try:
                location = geolocator.geocode(city_name)
                # If geocoding is successful, store latitude & longitude
                if location:
                    lat_lon = f"{location.latitude:.4f};{location.longitude:.4f}"
                else:
                    lat_lon = "NA;NA"
            except Exception:
                lat_lon = "NA;NA"

            # Write updated row
            writer.writerow({
                "city": city_name,       # quoted city name
                "location": lat_lon,            # e.g. 40.66;-73.94
                "population": population
            })

            # Be nice to the geocoding service; add a small pause
            time.sleep(1)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Populate latitude and longitude, given a csv file with city name.")
    parser.add_argument("src_file", help="Source csv file")
    parser.add_argument("dst_file", help="Destination csv file")
    args = parser.parse_args()

    # TODO: validate source file


    main(args.src_file, args.dst_file)