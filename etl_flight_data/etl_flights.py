#!/usr/bin/env python3
import argparse
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
    routes = Routes(airports)
    routes.process_routes_from_file("input_data/routes.dat")

    # load: results in files
    routes.save_results(args.output_file)

    logging.info("Process completed!")


if __name__ == "__main__":
    main()
