# RUN TEST OR COVERAGE
```
    make test
    make coverage
```

# fetch input data
```
bash scripts/fetch_input_data.sh
```

# Install
```
    python3 -m venv venv
    source venv/bin/activate
    pip install pip --upgrade
    make install

```

# Install airflow
```
    make install-airflow
```

# running locally airflow
```
    bash scripts/run_etl_flights_taskflow.sh
```
