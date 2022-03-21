DAGS := etl_flight_data
PWD := $(shell pwd)
TODAY := $(shell date -u +%Y%m%d)
# https://blog.emacsos.com/bootstrap-a-python-project.html

ifndef AIRFLOW_HOME
AIRFLOW_HOME := ~/airflow
endif

.PHONY: install install-dev test

install:
	pip install .

install-dev: install
	pip install -e .[dev]
	pre-commit install


clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -d -name '__pycache__' -exec rmdir {} +

clean-build:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info

clean: clean-pyc clean-build
	rm -rf mainenv __pycache__

install-airflow: clean
	# Pin dependencies according to https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#constraints-files
	pip install -r requirements_airflow.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-3.8.txt"
	rm -rf $(AIRFLOW_HOME)
	airflow db init

	# link
	ln -sfF $(PWD)/$(DAGS) $(AIRFLOW_HOME)/dags

	# airflow confi
	sed -i=.bak 's/load_examples = True/load_examples = False/g' $(AIRFLOW_HOME)/airflow.cfg
	sed -i=.bak "s/# AUTH_ROLE_PUBLIC = 'Public'/AUTH_ROLE_PUBLIC = 'Admin'/g" $(AIRFLOW_HOME)/webserver_config.py

	# link input data, for sensors checks
	ln -sfF  $(PWD)/input_data/ /tmp/

lint:
	pre-commit run --all-files

test:
	python3 -m pytest -v

coverage:
	coverage run -m pytest
	coverage report -m
	coverage report -m > coverage_report.log
	coverage html
	open htmlcov/index.html

fetch-input-data:
	./scripts/fetch_input_data.sh
