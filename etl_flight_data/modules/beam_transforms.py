import csv

import apache_beam as beam
from apache_beam import DoFn


class ParseCsv(DoFn):
    @staticmethod
    def parse_line(element):
        for line in csv.reader([element], quotechar='"', delimiter=",", quoting=csv.QUOTE_ALL, skipinitialspace=True):
            return line

    def process(self, element):
        return [ParseCsv.parse_line(element)]


class Airports(DoFn):
    @staticmethod
    def get_tuple(row):
        country = row[3]
        iata = row[4]
        return country, iata

    def process(self, element):
        _, rows = element
        data = {}
        x = []
        for row in rows:
            country, iata = Airports.get_tuple(row)
            data[iata] = country
            x.append((country, iata))
        return [data]

    @staticmethod
    def process_coll(airports_raw_coll):
        return (
            airports_raw_coll
            | "map_Airports" >> beam.Map(lambda e: (True, e))
            | "group" >> beam.GroupByKey()
            | "dict" >> beam.ParDo(Airports())
        )


class Routes(DoFn):
    @staticmethod
    def get_airports(element):
        source_airport = element[2]
        destination_airport = element[4]
        return source_airport, destination_airport

    @staticmethod
    def process_route(source_airport, destination_airport, airports_map):
        # get countries
        source_country = airports_map.get(source_airport)
        destination_country = airports_map.get(destination_airport)
        if source_country is None or destination_country is None:
            return
        is_domestic = source_country == destination_country
        result = source_country, is_domestic
        return result

    def process(self, element, airports_map):
        source_airport, destination_airport = self.get_airports(element)
        result = self.process_route(source_airport, destination_airport, airports_map)
        if result is None:
            return
        return [result]

    @staticmethod
    def process_coll(routes_coll, airports_coll):
        return (
            routes_coll
            | "Routes" >> beam.ParDo(Routes(), airports_map=beam.pvalue.AsSingleton(airports_coll))
            | "_SumFlightsFn" >> beam.CombinePerKey(_SumFlightsFn())
        )


class _SumFlightsFn(beam.CombineFn):
    def create_accumulator(self):
        domestic = 0
        international = 0
        accumulator = domestic, international
        return accumulator

    def add_input(self, accumulator, is_domestic):
        domestic, international = accumulator
        if is_domestic:
            domestic += 1
        else:
            international += 1
        return domestic, international

    def merge_accumulators(self, accumulators):
        domestics, internationals = zip(*accumulators)
        return sum(domestics), sum(internationals)

    def extract_output(self, accumulator):
        domestic, international = accumulator
        return domestic, international


class _FormatResultDoFn(DoFn):
    country_name_index = 0
    flights_index = 1

    def format_result(self, row):
        tup = row[self.country_name_index], *row[self.flights_index]
        return ",".join(map(str, tup))

    def process(self, element):
        return [self.format_result(element)]


class FormatResults:
    @staticmethod
    def _sort_values(element):
        _, data = element
        data.sort(key=lambda x: x[0])
        return data

    @staticmethod
    def _sort_coll(pcoll):
        "need to group gorup in one element to be able to sort"
        return (
            pcoll
            | "format_coll.AddKey" >> beam.Map(lambda e: (True, e))
            | "format_coll.GroupByKey" >> beam.GroupByKey()
            | "format_coll.sort_coll" >> beam.Map(FormatResults._sort_values)
            | "format_coll.FlatMap" >> beam.FlatMap(lambda elemens: (e for e in elemens))
        )

    @staticmethod
    def format_coll(pcoll):
        "need to group gorup in one element to be able to sort"
        results = FormatResults._sort_coll(pcoll) | "format_coll.FormatResults" >> beam.ParDo(_FormatResultDoFn())
        return results
