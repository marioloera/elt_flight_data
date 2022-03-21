import argparse
import logging

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

from etl_flight_data.modules.beam_transforms import (
    Airports,
    FormatResults,
    ParseCsv,
    Routes,
)


def run(output_file, airports_file, routes_file, pipeline_args=None):

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    p = Pipeline(options=pipeline_options)

    airports_coll = (
        p
        | "ReadFromText.Airports" >> ReadFromText(file_pattern=airports_file)
        | "ParseCsvAirpots" >> beam.ParDo(ParseCsv())
    )
    routes_coll = (
        p | "ReadFromText.Routes" >> ReadFromText(file_pattern=routes_file) | "ParseCsvRoutes" >> beam.ParDo(ParseCsv())
    )

    processed_airports = Airports.process_coll(airports_coll)
    processed_routes = Routes.process_coll(routes_coll, processed_airports)

    sorted_coll = FormatResults.format_coll(processed_routes)

    sorted_coll | "ResultsWriteToText" >> WriteToText(
        file_path_prefix=output_file,
        shard_name_template="",
    )
    _ = p.run()


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--log_level", type=int, help="Python log level", default=30)
    parser.add_argument("--output_file", default="output_data/output.csv")
    parser.add_argument("--airports_file", default="input_data/airports.dat")
    parser.add_argument("--routes_file", default="input_data/routes.dat")

    known_args, pipeline_args = parser.parse_known_args()
    logging.getLogger().setLevel(known_args.log_level)
    logging.info(f"known_args:{known_args}")
    logging.info(f"pipeline_args:{pipeline_args}")

    run(
        known_args.output_file,
        known_args.airports_file,
        known_args.routes_file,
        pipeline_args,
    )


if __name__ == "__main__":
    main()
