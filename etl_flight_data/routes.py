import csv
import logging


class Routes:
    def __init__(self, airports):
        self.airports = airports
        self.flights_per_country = {}

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
            return None
        try:
            source_airport = row[self.columns["source_airport"]]
            destination_airport = row[self.columns["destination_airport"]]

            # get countries
            source_country = self.airports.get(source_airport, None)
            destination_country = self.airports.get(destination_airport, None)
            if source_country is None or destination_country is None:
                return None

            is_domestic = source_country == destination_country
            return source_country, is_domestic

        except Exception as ex:
            msg = f"{ex}. row: {row}"
            logging.warning(msg)
            return None

    def acc_route(self, processed_route):
        if processed_route is None:
            return
        # check if is domestic or international flight
        source_country, is_domestic = processed_route

        # add country to countries dictionary
        if source_country not in self.flights_per_country:
            self.flights_per_country[source_country] = {
                "domestic_count": 0,
                "international_count": 0,
            }

        # add flights to countries
        self.flights_per_country[source_country]["domestic_count"] += int(is_domestic)
        self.flights_per_country[source_country]["international_count"] += int(not is_domestic)

    def process_routes(self, routes):
        for route in routes:
            self.acc_route(self.process_route(route))

    def get_formated_results(self):
        results = [
            (c, self.flights_per_country[c]["domestic_count"], self.flights_per_country[c]["international_count"])
            for c in sorted(self.flights_per_country.keys())
        ]
        return results

    @staticmethod
    def get_flights_per_country(aiports, file_path):
        logging.info(f"getting routes from {file_path} file")
        routes = Routes(aiports)
        with open(file_path, "r", encoding="UTF-8") as f:
            reader = csv.reader(f)
            for row in reader:
                routes.acc_route(routes.process_route(row))

        return routes.get_formated_results()
