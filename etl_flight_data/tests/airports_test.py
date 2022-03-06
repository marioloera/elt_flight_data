from etl_flight_data.airports import Airports


class TestAirports:
    input_data = [
        (3484, "Los Angeles International Airport", "Los Angeles", "United States", "LAX"),
        (737, "Stockholm-Arlanda Airport", "Stockholm", "Sweden", "ARN"),
    ]

    def test_process_row(self):
        assert "ARN", "Sweden" == Airports._process_row(self.input_data[0])
        assert "LAX", "United States" == Airports._process_row(self.input_data[1])

    def test_process_row_invalid(self):
        assert Airports._process_row(("c1", "c2")) is None
        assert Airports._process_row(None) is None
        assert Airports._process_row("abc,iji,idk") is None
        assert Airports._process_row({1: "ab"}) is None

    def test_process_rows(self):
        expected = {
            "ARN": "Sweden",
            "LAX": "United States",
        }
        actual_data = Airports.process_rows(self.input_data)
        assert expected == actual_data

    def test_procss_file(self):
        file_path = "test_data/airports.dat"
        expected = {
            "GKA": "Papua New Guinea",
            "LAE": "Papua New Guinea",
            "OBL": "Belgium",
            "AOC": "Germany",
        }
        actual_data = Airports.process_file(file_path)
        assert expected == actual_data
