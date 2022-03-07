from etl_flight_data.routes import Routes


class TestRoutes:
    AIRPORTS = {
        "ARN": "Sweden",
        "GOT": "Sweden",
        "LAX": "United States",
        "ELP": "United States",
    }

    def test_process_route(self):
        routes = Routes(self.AIRPORTS)
        assert ("Sweden", True) == routes.process_route((00, 000, "ARN", 737, "GOT"))
        assert ("Sweden", False) == routes.process_route((00, 000, "ARN", 737, "LAX"))

    def test_process_route_unkwnon_country(self):
        routes = Routes(self.AIRPORTS)
        assert (None) == routes.process_route((00, 410, "XXX", -1, "YYY"))
        assert (None) == routes.process_route((00, 410, "GOT", -1, "YYY"))

    def test_process_route_invalid_input(self):
        routes = Routes(self.AIRPORTS)
        assert (None) == routes.process_route('00, 000, "ARN", 737, "GOT"')
        assert (None) == routes.process_route((00, 000))
        assert (None) == routes.process_route(None)

    def test_acc_route(self):

        processed_routes = [
            ("Sweden", True),
            ("Sweden", True),
            ("Sweden", False),
            ("United States", True),
            None,
        ]
        expected_output = {
            "Sweden": {
                "domestic_count": 2,
                "international_count": 1,
            },
            "United States": {
                "domestic_count": 1,
                "international_count": 0,
            },
        }

        routes = Routes(self.AIRPORTS)
        for processed_route in processed_routes:
            routes.acc_route(processed_route)

        assert expected_output == routes.flights_per_country

    def test_process_routes(self):

        routes_data = [
            (00, 000, "ARN", 737, "GOT"),
            (00, 000, "ARN", 737, "GOT"),
            (00, 000, "ARN", 737, "LAX"),
            (00, 000, "LAX", 737, "GOT"),
            (00, 000, "LAX", 737, "ARN"),
            (00, 000, "LAX", 737, "ELP"),
        ]
        expected_output = {
            "Sweden": {
                "domestic_count": 2,
                "international_count": 1,
            },
            "United States": {
                "domestic_count": 1,
                "international_count": 2,
            },
        }

        routes = Routes(self.AIRPORTS)
        routes.process_routes(routes_data)
        assert expected_output == routes.flights_per_country

    def test_get_formated_results(self):
        routes = Routes({})
        routes.flights_per_country = {
            "Sweden": {
                "domestic_count": 2,
                "international_count": 1,
            },
            "United States": {
                "domestic_count": 1,
                "international_count": 2,
            },
        }
        expected_output = [
            ("Sweden", 2, 1),
            ("United States", 1, 2),
        ]
        assert expected_output == routes.get_formated_results()

    def test_get_flights_per_country(self):
        flights_per_country_file = Routes.get_flights_per_country(self.AIRPORTS, "test_data/routes.dat")
        expected_output = [
            ("Sweden", 2, 1),
            ("United States", 1, 2),
        ]
        assert expected_output == flights_per_country_file
