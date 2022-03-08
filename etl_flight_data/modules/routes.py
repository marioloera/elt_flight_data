import csv
import logging

from modules.countries import Countries


class Routes:
    def __init__(self, airports):
        self.airports = airports
        self.countries = Countries()

    columns = dict(
        airline=0,
        airline_id=1,
        source_airport=2,
        source_airport_id=3,
        destination_airport=4,
        destination_airport_id=5,
        codeshare=6,
        stops=7,
        equipment=8,
    )

    def process_route(self, row):
        """
        Process each flight, get the airport countries
        and add the number of domestic and international flights
        to the countries dictionary from the source country.
        The flights where the source or destination airports are
        missing in airports_by_iata dictionary will be added to the
        unknown country record
        """
        # get row info
        if not (isinstance(row, list) or isinstance(row, tuple)):
            logging.warning(f"wrong input type: {type(row)}")
            return
        try:
            source_airport = row[self.columns["source_airport"]]
            destination_airport = row[self.columns["destination_airport"]]

            # get countries
            source_country = self.airports.get(source_airport)
            destination_country = self.airports.get(destination_airport)
            if source_country is None or destination_country is None:
                return

            is_domestic = source_country == destination_country
            return source_country, is_domestic

        except Exception as ex:
            msg = f"{ex}. row: {row}"
            logging.warning(msg)
            return

    @staticmethod
    def get_flights_per_country(aiports, file_path):
        logging.info(f"getting routes from {file_path} file")
        routes = Routes(aiports)
        with open(file_path, "r", encoding="UTF-8") as f:
            reader = csv.reader(f)
            for row in reader:
                processed_route = routes.process_route(row)
                if processed_route is None:
                    continue
                routes.countries.acc_country(*processed_route)

        return routes.countries.get_formated_results()
