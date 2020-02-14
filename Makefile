help:
	@echo "clean - remove Python and Airflow debris"
	@echo "venv - install venv with all requirements for Airflow"
	@echo "up - bring up Airflow Docker compose infrastructure"
	@echo "down - tear down Airflow Docker compose infrastructure"
	@echo "restart = down then up"
	@echo "logs - print, then follow the Airflow logs as they come"

default: help

clean-airflow:
	find ./airflow_home -mindepth 1 -maxdepth 1 \
	! \( -name 'dags' -or -name 'plugins' -or -name '__init__.py' \) \
 	-exec rm -rf {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean: clean-airflow clean-pyc

venv:
	@if [ ! -e "./venv/bin/activate" ] ; then \
		python3 -m venv ./venv ; \
		. ./venv/bin/activate; \
		pip install -r requirements.txt; \
		pip install --upgrade pip ; \
		deactivate; \
	else \
		echo "Virtual environment had already been created." ; \
	fi

init: venv
	export AIRFLOW_HOME=`pwd`/airflow_home ; \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False ; \
	. ./venv/bin/activate ; \
	airflow initdb ; \
	airflow connections -a --conn_id livy --conn_type HTTP \
	--conn_host localhost --conn_schema http --conn_port 8998 ; \
	airflow connections -a --conn_id spark --conn_type HTTP \
	--conn_host localhost --conn_schema http --conn_port 18080 ; \
	airflow variables -s pyspark_path `pwd`/pyspark ; \
	deactivate ;

up:
	export AIRFLOW_HOME=`pwd`/airflow_home ; \
	export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8888 ; \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False ; \
	. ./venv/bin/activate ; \
	airflow scheduler & \
	airflow webserver ; \
	deactivate ;

down:
	pkill -f airflow | true