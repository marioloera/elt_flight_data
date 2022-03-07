"""
    this dag create three task:
    get_airpots >> get_flights_per_country >> write_to_csv
    using TaskFlow API paradigm
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
"""
import csv
from datetime import datetime

from airflow.decorators import dag, task
from airports import Airports
from routes import Routes


@dag(
    start_date=datetime(2022, 3, 7),
    catchup=False,
    schedule_interval="@daily",
    tags=["test"],
)
def etl_flights_taskflow():
    @task()
    def get_airports(airports_file):
        return Airports.get_airports(airports_file)

    @task()
    def get_flights_per_country(airports, routes_file):
        return Routes.get_flights_per_country(airports, routes_file)

    @task()
    def write_to_csv(flights_per_country, output_file):
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(flights_per_country)

    # alternative 1: create only one etl task that do all the logic
    # alternative 2: create only one task that call etl_flights.run_etl

    airports = get_airports("input_data/airports.dat")
    flights_per_country = get_flights_per_country(airports, "input_data/routes.dat")
    write_to_csv(flights_per_country, "output_data/output.csv")


_ = etl_flights_taskflow()
