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


def main(
    output_file="output_data/output.csv",
    airports_file="input_data/airports.dat",
    routes_file="input_data/routes.dat",
    pipeline_args=None,
):

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


if __name__ == "__main__":
    main()
