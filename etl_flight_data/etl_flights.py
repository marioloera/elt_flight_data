import argparse
import csv
import logging

from etl_flight_data.modules.airports import Airports
from etl_flight_data.modules.routes import Routes


def run_etl(
    output_file="output_data/output.csv",
    airpots_file="input_data/airports.dat",
    routes_file="input_data/routes.dat",
):
    logging.info("etl sarted!")

    # extract transform: airports
    airports = Airports.get_airports(airpots_file)

    # extract transform: routes
    flights_per_country = Routes.get_flights_per_country(airports, routes_file)

    # load: results in files
    save_results(flights_per_country, output_file)

    logging.info("etl completed!")


def save_results(results, output_file):
    logging.info(f"writing results to {output_file}")
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(results)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_file",
        help="output file where results are stored",
        default="output_data/output.csv",
    )
    args = parser.parse_args()

    logging.info(args)

    run_etl(args.output_file)


if __name__ == "__main__":
    main()
