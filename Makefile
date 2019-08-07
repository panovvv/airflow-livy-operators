help:
	@echo "clean - remove Python artifacts, virtual environment and IDE project files"
	@echo "venv - install venv with all requirements for Airflow"
	@echo "up - bring up Airflow Docker compose infrastructure"
	@echo "down - tear down Airflow Docker compose infrastructure"
	@echo "restart - tear down and bring up again Airflow Docker compose infrastructure"

all: venv up

default: help

clean: clean-venv clean-pyc

clean-venv:
	rm -rf venv/

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

venv:
	@if [ ! -e "./venv/bin/activate" ] ; then \
		python3 -m venv ./venv ; \
		. ./venv/bin/activate && \
		pip install -r requirements.txt ; \
	else \
		echo "Virtual environment had already been created." ; \
	fi

up:
	docker-compose up -d

	docker-compose -f docker-compose.yml run \
	--rm webserver airflow connections -a --conn_id livy --conn_type HTTP \
	--conn_host localhost --conn_schema http --conn_port 8998

	docker-compose -f docker-compose.yml run \
	--rm webserver airflow connections -a --conn_id spark --conn_type HTTP \
	--conn_host localhost --conn_schema http --conn_port 18080

logs:
	docker-compose logs -f webserver

down:
	docker-compose down

restart: down up