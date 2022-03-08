from modules.countries import Countries, Country


class TestCountry:
    def test_init(self):
        country1 = Country("foo")
        assert country1.domestic_flight_count == 0
        assert country1.international_flight_count == 0

    def test_update_flight_count(self):
        country1 = Country("foo")

        # add domestic flight
        country1.update_flight_count(is_domestic_flight=True)
        assert country1.domestic_flight_count == 1
        assert country1.international_flight_count == 0

        # add domestic flight
        country1.update_flight_count(is_domestic_flight=True)
        assert country1.domestic_flight_count == 2
        assert country1.international_flight_count == 0

        # add international flight
        country1.update_flight_count(is_domestic_flight=False)
        assert country1.domestic_flight_count == 2
        assert country1.international_flight_count == 1

        # add international flight
        country1.update_flight_count(is_domestic_flight=False)
        assert country1.domestic_flight_count == 2
        assert country1.international_flight_count == 2

    def test_get_touple(self):
        country1 = Country("foo")
        expected_result = "foo", 0, 0
        assert expected_result == country1.get_touple()


class TestCountries:
    def test_init(self):
        countries1 = Countries()
        assert countries1.data == {}

    def test_acc_country(self):
        countries1 = Countries()
        inputs = [
            ("Sweden", True),
            ("Sweden", True),
            ("Sweden", False),
            ("United States", True),
        ]

        for input in inputs:
            country_name, is_domestic_flight = input
            countries1.acc_country(country_name, is_domestic_flight)

        assert countries1.data["Sweden"].domestic_flight_count == 2
        assert countries1.data["Sweden"].international_flight_count == 1
        assert countries1.data["United States"].domestic_flight_count == 1
        assert countries1.data["United States"].international_flight_count == 0

    def test_get_formated_results(self):
        countries1 = Countries()
        countries1.acc_country("Sweden", False)
        countries1.acc_country("United States", True)
        expected_output = [
            ("Sweden", 0, 1),
            ("United States", 1, 0),
        ]
        assert expected_output == countries1.get_formated_results()

        countries1.acc_country("Sweden", False)
        expected_output = [
            ("Sweden", 0, 2),
            ("United States", 1, 0),
        ]
        assert expected_output == countries1.get_formated_results()
