help:
	@echo "clean - remove Python and Airflow debris                        TODO UPDATE"
	@echo "venv - install venv with all requirements for Airflow"
	@echo "up - bring up Airflow Docker compose infrastructure"
	@echo "down - tear down Airflow Docker compose infrastructure"
	@echo "restart = down then up"
	@echo "logs - print, then follow the Airflow logs as they come"

default: help

rm-trash:
	find ./airflow_home -mindepth 1 -maxdepth 1 \
	! \( -name 'dags' -or -name 'plugins' -or -name '__init__.py' \) \
 	-exec rm -rf {} + ; \
	find . -path ./venv -prune -o \
	\( -name 'metastore_db' -or -name 'derby.log' \
	 -or -name 'build' -or -name 'dist' -or -name '*.egg-info' \) -exec rm -rf {} + ; \
	find . -path ./venv -prune -o \
	\( -name '*.pyc' -or  -name '*.pyo' -or -name '*~' \
	-or -name '__pycache__' -or -name '.pytest_cache' -or -name '.tox' \) \
 	-exec rm -rf {} +

rm-venv:
	rm -rf ./venv

clean: rm-trash rm-venv

venv:
	@if [ ! -e "./venv/bin/activate" ] ; then \
		python3 -m venv ./venv ; \
		. ./venv/bin/activate; \
		python3 -m pip install --upgrade pip ; \
		python3 -m pip install -r requirements.txt; \
		deactivate; \
	else \
		echo "Virtual environment had already been created." ; \
	fi

init: venv
	@if [ ! -e "./airflow_home/airflow.db" ] ; then \
		export AIRFLOW_HOME=`pwd`/airflow_home ; \
		export AIRFLOW__CORE__LOAD_EXAMPLES=False ; \
		. ./venv/bin/activate ; \
		airflow initdb ; \
		airflow variables -s session_files_path `pwd`/sessions ; \
		airflow variables -s batch_files_path `pwd`/batches ; \
		airflow connections -a --conn_id livy --conn_type HTTP \
		--conn_host localhost --conn_schema http --conn_port 8998 ; \
		airflow connections -a --conn_id spark --conn_type HTTP \
		--conn_host localhost --conn_schema http --conn_port 18080 ; \
		airflow connections -a --conn_id yarn --conn_type HTTP \
		--conn_host localhost --conn_schema http --conn_port 8088 ; \
		airflow unpause 01_session_example ; \
		airflow unpause 02_session_example_load_from_file ; \
		airflow unpause 03_batch_example ; \
		airflow unpause 04_batch_example_failing ; \
		airflow unpause 05_batch_example_verify_in_spark ; \
		airflow unpause 06_batch_example_verify_in_yarn ; \
		deactivate ; \
	else \
		echo "Airflow had already been initialized." ; \
	fi

copy-batches:
	rsync -arv ./batches/*.py ~/data/vpanov/bigdata-docker-compose/data/batches/

up: init copy-batches
	export AIRFLOW_HOME=`pwd`/airflow_home ; \
	export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8888 ; \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False ; \
	. ./venv/bin/activate ; \
	airflow scheduler & \
	airflow webserver ; \
	deactivate ;

down:
	pkill -f airflow | true

init-dev:
	@if [ ! -e "./venv/bin/pytest" ] ; then \
		. ./venv/bin/activate ; \
		python3 -m pip install --upgrade pip setuptools wheel ; \
		python3 -m pip install -r requirements_dev.txt; \
		deactivate ; \
	else \
		echo "Dev env had already been initialized." ; \
	fi

tests: init-dev
	. ./venv/bin/activate ; \
	pytest ; \
	deactivate ;

codecov: init-dev
	. ./venv/bin/activate ; \
	pytest --cov=airflow_home.plugins ; \
	deactivate ;
