# airflow dags list

airflow tasks list -t etl_flights_taskflow

# since the task use previus task returns values
# they need to be executed in order

airflow tasks test etl_flights_taskflow get_airports 20220301

airflow tasks test etl_flights_taskflow get_flights_per_country 20220301

airflow tasks test etl_flights_taskflow save_results_task 20220301
