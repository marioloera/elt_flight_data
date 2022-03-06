from etl_flight_data.transforms import Airports


class TestAirports:
    input_data = [
        (
            3484,
            "Los Angeles International Airport",
            "Los Angeles",
            "United States",
            "LAX",
            "KLAX",
            33.94250107,
            -118.4079971,
            125,
            -8,
            "A",
            "America/Los_Angeles",
            "airport",
            "OurAirports",
        ),
        (
            737,
            "Stockholm-Arlanda Airport",
            "Stockholm",
            "Sweden",
            "ARN",
            "ESSA",
            59.651901245117,
            17.918600082397,
            137,
            1,
            "E",
            "Europe/Stockholm",
            "airport",
            "OurAirports",
        ),
    ]

    def test_process_row(self):
        assert "ARN", "Sweden" == Airports._process_row(self.input_data[0])
        assert "LAX", "United States" == Airports._process_row(self.input_data[1])

    def test_process_row_invalid(self):
        assert Airports._process_row(("c1", "c2")) is None
        assert Airports._process_row(None) is None
        assert Airports._process_row("abc,iji,idk") is None
        assert Airports._process_row(list(self.input_data[1])) is None

    def test_process_rows(self):
        expected = {
            "ARN": "Sweden",
            "LAX": "United States",
        }
        actual_data = Airports().process_rows(self.input_data)
        assert expected == actual_data
