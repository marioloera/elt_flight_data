.PHONY: install install-dev test

install:
	pip install -r requirements.txt

install-dev: install
	pre-commit install

lint:
	pre-commit run --all-files

test:
	python3 -m pytest -v

coverage:
	coverage run -m pytest
	coverage report -m
	coverage html
	open htmlcov/index.html

fetch-input-data:
	./scripts/fetch_input_data.sh
