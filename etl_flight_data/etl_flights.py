import argparse
import csv
import logging

from airports import Airports
from routes import Routes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_file",
        help="output file where results are stored",
        default="output_data/output.csv",
    )
    args = parser.parse_args()
    logging.info("Process sarted!")
    logging.info(args)

    # extract transform: airports
    airports = Airports.process_file("input_data/airports.dat")

    # extract transform: routes
    flights_per_country = Routes.get_flights_per_country(airports, "input_data/routes.dat")

    # load: results in files
    with open(args.output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(flights_per_country)

    logging.info("Process completed!")


if __name__ == "__main__":
    main()
