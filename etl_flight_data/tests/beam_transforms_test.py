import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from modules.beam_transforms import (
    Airports,
    FormatResults,
    ParseCsv,
    Routes,
    _FormatResultDoFn,
    _SumFlightsFn,
)


class TestAirports:
    input_data = [
        (3484, "Los Angeles International Airport", "Los Angeles", "United States", "LAX"),
        (737, "Stockholm-Arlanda Airport", "Stockholm", "Sweden", "ARN"),
    ]
    expected_output = {
        "ARN": "Sweden",
        "LAX": "United States",
    }

    def test_get_tuple(self):
        assert "ARN", "Sweden" == Airports.test_get_tuple(self.input_data[0])
        assert "LAX", "United States" == Airports.test_get_tuple(self.input_data[1])

    def test_process(self):
        grouped_data = [(True, self.input_data)]
        with TestPipeline() as p:
            actual_output = p | beam.Create(grouped_data) | beam.ParDo(Airports())
            assert_that(actual_output, equal_to([self.expected_output]))

    def test_process_coll(self):
        with TestPipeline() as p:
            airports_coll = p | beam.Create(self.input_data)
            actual_output = Airports.process_coll(airports_coll)
            assert_that(actual_output, equal_to([self.expected_output]))


class TestParseCsv:
    input_data = [
        '1,2,"three","IV"',
        '"a","B","C",,"E"',
    ]
    expected_output = [
        ["1", "2", "three", "IV"],
        ["a", "B", "C", "", "E"],
    ]

    def test_parse_line(self):
        for input, expected in zip(self.input_data, self.expected_output):
            assert expected == ParseCsv.parse_line(input)

    def test_process(self):
        with TestPipeline() as p:
            actual_output = p | beam.Create(self.input_data) | beam.ParDo(ParseCsv())
            assert_that(actual_output, equal_to(self.expected_output))


class Test_FormatResultDoFn:
    input_data = [
        ("Sweden", (1, 2)),
        ("Canada", (3, 0)),
    ]
    expected_output = [
        "Sweden,1,2",
        "Canada,3,0",
    ]

    def test_format_result(self):
        for input, expected in zip(self.input_data, self.expected_output):
            assert expected == _FormatResultDoFn().format_result(input)

    def test_process(self):
        with TestPipeline() as p:
            actual_output = p | beam.Create(self.input_data) | beam.ParDo(_FormatResultDoFn())
            assert_that(actual_output, equal_to(self.expected_output))


class TestRoutesBeam:
    AIRPORTS = {
        "ARN": "Sweden",
        "GOT": "Sweden",
        "LAX": "United States",
        "ELP": "United States",
    }

    def test_get_airports(self):
        assert ("ARN", "GOT") == Routes.get_airports((00, 000, "ARN", 737, "GOT"))

    def test_get_airports_none(self):
        assert ("ARN", None) == Routes.get_airports((00, 000, "ARN", 737, None))
        assert (None, "ARN") == Routes.get_airports((00, 000, None, 737, "ARN"))
        assert (None, None) == Routes.get_airports((00, 000, None, 737, None))

    def test_process_route(self):
        assert ("Sweden", True) == Routes.process_route("ARN", "GOT", self.AIRPORTS)
        assert ("Sweden", False) == Routes.process_route("ARN", "LAX", self.AIRPORTS)

    def test_process_route_none(self):
        assert (None) == Routes.process_route("GOT", "XXX", self.AIRPORTS)
        assert (None) == Routes.process_route("XXX", "GOT", self.AIRPORTS)
        assert (None) == Routes.process_route("XXX", "YYY", self.AIRPORTS)
        assert (None) == Routes.process_route(None, "YYY", self.AIRPORTS)
        assert (None) == Routes.process_route("XXX", None, self.AIRPORTS)
        assert (None) == Routes.process_route(None, None, self.AIRPORTS)
        assert (None) == Routes.process_route("XZT", "ABC", {})

    def test_process_coll(self):
        input_data = [
            (00, 000, "ARN", 737, "GOT"),
            (00, 000, "LAX", 737, "ARN"),
            (00, 000, "XXX", 737, "ARN"),
            (00, 000, "LAX", 737, "ELP"),
        ]

        expected_output = [
            ("Sweden", (1, 0)),
            ("United States", (1, 1)),
        ]

        with TestPipeline() as p:
            airports_coll = p | "airports_coll" >> beam.Create([self.AIRPORTS])
            input_coll = p | beam.Create(input_data)
            actual_output = Routes.process_coll(input_coll, airports_coll)
            assert_that(actual_output, equal_to(expected_output))


class Test_SumFlightsFn:
    input_data = [
        ("A", True),
        ("A", True),
        ("A", False),
        ("B", False),
    ]

    expected_output = [
        ("A", (2, 1)),
        ("B", (0, 1)),
    ]
    with TestPipeline() as p:
        actual_output = p | beam.Create(input_data) | beam.CombinePerKey(_SumFlightsFn())
        assert_that(actual_output, equal_to(expected_output))


class TestFormatResults:
    input_data = [
        ("Sweden", (0, 1)),
        ("Mexico", (2, 3)),
        ("Canada", (4, 5)),
    ]
    group_input_data = (
        True,
        input_data,
    )

    expected_output = [
        ("Canada", (4, 5)),
        ("Mexico", (2, 3)),
        ("Sweden", (0, 1)),
    ]

    def test_sort_values(self):
        assert self.expected_output == FormatResults._sort_values(self.group_input_data)

    def test_sort_coll(self):
        with TestPipeline() as p:
            input_coll = p | beam.Create(self.input_data)
            actual_output = FormatResults._sort_coll(input_coll)
            assert_that(actual_output, equal_to(self.expected_output))

    def test_format_coll(self):
        expected_output_str = [
            ("Canada,4,5"),
            ("Mexico,2,3"),
            ("Sweden,0,1"),
        ]
        with TestPipeline() as p:
            input_coll = p | beam.Create(self.input_data)
            actual_output = FormatResults.format_coll(input_coll)
            assert_that(actual_output, equal_to(expected_output_str))
