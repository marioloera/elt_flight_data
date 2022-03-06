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
        try:
            source_airport = row[self.columns["source_airport"]]
            destination_airport = row[self.columns["destination_airport"]]

            # get countries
            source_country = self.airports.get(source_airport, None)
            destination_country = self.airports.get(destination_airport, None)
            if source_country is None or destination_country is None:
                return None, None

            is_domestic = source_country == destination_country
            return source_country, is_domestic

        except Exception as ex:
            msg = f"{ex}. row: {row}"
            logging.warning(msg)
            pass

    def acc_route(self, processed_route):
        # check if is domestic or international flight
        source_country, is_domestic = processed_route

        # split here
        # add country to countries dictionary
        if source_country not in self.flights_per_country:
            self.flights_per_country[source_country] = {
                "domestic_count": 0,
                "international_count": 0,
            }

        # add flights to countries
        self.flights_per_country[source_country]["domestic_count"] += int(is_domestic)
        self.flights_per_country[source_country]["international_count"] += int(not is_domestic)
