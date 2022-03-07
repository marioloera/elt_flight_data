"""
    this dag create three task:
    get_airpots >> get_flights_per_country >> write_to_csv
    using TaskFlow API paradigm
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
"""
from datetime import datetime

from airflow.decorators import dag, task
from airports import Airports
from etl_flights import save_results
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
    def save_results_task(flights_per_country, output_file):
        save_results(flights_per_country, output_file)

    # alternative 1: create only one etl task that do all the logic
    # alternative 2: create only one task that call etl_flights.run_etl

    airports_file = "input_data/airports.dat"
    routes_file = "input_data/routes.dat"
    output_file = "output_data/output.csv"

    airports = get_airports(airports_file)
    flights_per_country = get_flights_per_country(airports, routes_file)
    save_results_task(flights_per_country, output_file)


_ = etl_flights_taskflow()
