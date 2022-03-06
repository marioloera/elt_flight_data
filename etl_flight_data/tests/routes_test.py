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
        assert (None, None) == routes.process_route((00, 410, "XXX", -1, "YYY"))
        assert (None, None) == routes.process_route((00, 410, "GOT", -1, "YYY"))

    def test_acc_route(self):

        processed_routes = [
            ("Sweden", True),
            ("Sweden", True),
            ("Sweden", False),
            ("United States", True),
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
