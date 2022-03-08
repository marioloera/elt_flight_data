from modules.routes import Routes


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

    def test_get_flights_per_country(self):
        flights_per_country_file = Routes.get_flights_per_country(self.AIRPORTS, "test_data/routes.dat")
        expected_output = [
            ("Sweden", 2, 1),
            ("United States", 1, 2),
        ]
        assert expected_output == flights_per_country_file
