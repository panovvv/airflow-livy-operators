#!/usr/bin/env sh

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd -P)

init_dev() {
  if [ -f "${SCRIPT_DIR}/venv/requirements_dev.txt" ]; then
    if diff "${SCRIPT_DIR}/requirements_dev.txt" "${SCRIPT_DIR}/venv/requirements_dev.txt"; then
      echo "requirements_dev.txt did not change since we last installed it"
    else
      echo "requirements_dev.txt changed since we last installed it"
      . "${SCRIPT_DIR}/venv/bin/activate"
      python3 -m pip install -r "${SCRIPT_DIR}/requirements_dev.txt"
      cp "${SCRIPT_DIR}/requirements_dev.txt" "${SCRIPT_DIR}/venv/requirements_dev.txt"
      deactivate
    fi
  else
    echo "Installing dev dependencies for the first time..."
    . "${SCRIPT_DIR}/venv/bin/activate"
    python3 -m pip install --upgrade setuptools wheel
    python3 -m pip install -r "${SCRIPT_DIR}/requirements_dev.txt"
    cp "${SCRIPT_DIR}/requirements_dev.txt" "${SCRIPT_DIR}/venv/requirements_dev.txt"
    deactivate
  fi
}

export_airflow_env () {
  AIRFLOW_HOME="${SCRIPT_DIR}/airflow_home"
  export AIRFLOW_HOME
  export AIRFLOW__CORE__LOAD_EXAMPLES=False
  export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8888
}

init_airflow() {
  if [ ! -e "${SCRIPT_DIR}/airflow_home/airflow.db" ]; then
    . "${SCRIPT_DIR}/venv/bin/activate"
    airflow initdb
    airflow variables -s session_files_path "${SCRIPT_DIR}/sessions"
    airflow variables -s batch_files_path "${SCRIPT_DIR}/batches"
    airflow connections -a --conn_id livy --conn_type HTTP \
      --conn_host localhost --conn_schema http --conn_port 8998
    airflow connections -a --conn_id spark --conn_type HTTP \
      --conn_host localhost --conn_schema http --conn_port 18080
    airflow connections -a --conn_id yarn --conn_type HTTP \
      --conn_host localhost --conn_schema http --conn_port 8088
    airflow unpause 01_session_example
    airflow unpause 02_session_example_load_from_file
    airflow unpause 03_batch_example
    airflow unpause 04_batch_example_failing
    airflow unpause 05_batch_example_verify_in_spark
    airflow unpause 06_batch_example_verify_in_yarn
    deactivate
    echo "Airflow initialized."
  else
    echo "Airflow had already been initialized."
  fi
}

copy_batches() {
  TO_DIR="$HOME/data/vpanov/bigdata-docker-compose/data/batches/"
  rsync -arv "${SCRIPT_DIR}"/batches/*.py "${TO_DIR}"
  echo "Copied the batch files to ${TO_DIR}"
}

create_venv() {
  if [ ! -e "${SCRIPT_DIR}/venv/bin/activate" ]; then
    echo "Creating virtual environment for the first time..."
    python3 -m venv "${SCRIPT_DIR}/venv"
    . "${SCRIPT_DIR}/venv/bin/activate"
    python3 -m pip install --upgrade pip
    python3 -m pip install -r "${SCRIPT_DIR}/requirements.txt"
    cp "${SCRIPT_DIR}/requirements.txt" "${SCRIPT_DIR}/venv/requirements.txt"
    deactivate
  else
    echo "Virtual environment had already been created."
    if [ -f "${SCRIPT_DIR}/venv/requirements.txt" ]; then
      if diff "${SCRIPT_DIR}/requirements.txt" "${SCRIPT_DIR}/venv/requirements.txt"; then
        echo "requirements.txt did not change since we last installed it"
      else
        echo "requirements.txt changed since we last installed it"
        . "${SCRIPT_DIR}/venv/bin/activate"
        python3 -m pip install -r "${SCRIPT_DIR}/requirements.txt"
        cp "${SCRIPT_DIR}/requirements.txt" "${SCRIPT_DIR}/venv/requirements.txt"
        deactivate
      fi
    else
      echo "requirements.txt does not exist in venv folder"
      cp "${SCRIPT_DIR}/requirements.txt" "${SCRIPT_DIR}/venv/requirements.txt"
    fi
  fi
}

remove_trash() {
  find "${SCRIPT_DIR}/airflow_home" -mindepth 1 -maxdepth 1 \
    ! \( -name 'dags' -or -name 'plugins' -or -name '__init__.py' \) \
    -delete
  find "${SCRIPT_DIR}" -path "${SCRIPT_DIR}/venv" -prune -o \
    \( -name 'metastore_db' -or -name 'derby.log' \
    -or -name 'coverage.xml' -or -name '.coverage' -or -name 'htmlcov' \
    -or -name 'build' -or -name 'dist' -or -name '*.egg-info' \) -delete
  find "${SCRIPT_DIR}" -path "${SCRIPT_DIR}/venv" -prune -o \
    \( -name '*.pyc' -or -name '*.pyo' -or -name '*~' \
    -or -name '__pycache__' -or -name '.pytest_cache' -or -name '.tox' \) \
    -delete
  echo "All of the extra files had been deleted!"
}

show_help() {
  echo "Use case 1: bring up Airflow at http://localhost:8888/admin/ and run test DAGs:"
  echo "            ./airflow.sh up"
  echo "Use case 2: kill all Airflow processes:"
  echo "            ./airflow.sh down"
  echo "Use case 3.1: prepare development environment:"
  echo "            ./airflow.sh dev"
  echo "Use case 3.2: run tests and generate HTML coverage report:"
  echo "            ./airflow.sh cov"
  echo
  echo "Full list of commands:"
  echo "rm-trash   -   remove Python and Airflow debris"
  echo "clean      -   previous + delete virtual environment, i.e. full cleaning."
  echo "venv       -   install virtual environment with all requirements for Airflow and PySpark"
  echo "init       -   prep Airflow database, variables, connections etc."
  echo "batches    -   copy the batches from batches/ to where Spark cluster can reach them.)"
  echo "               Re-define as needed (e.g. aws s3 cp batches/ s3:/dev/pyspark)"
  echo "up         -   bring up Airflow"
  echo "down       -   tear down Airflow"
  echo "dev        -   prepare development environment."
  echo "cov        -   run tests and generate HTML coverage report."
}

case "$1" in

cov)
  create_venv
  init_dev
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  pytest --cov=airflow_home.plugins --cov-report=html
  deactivate
  ;;

dev)
  create_venv
  init_dev
  ;;

up)
  copy_batches
  create_venv
  export_airflow_env
  init_airflow
  . "${SCRIPT_DIR}/venv/bin/activate"
  airflow scheduler &
  airflow webserver
  deactivate
  ;;

down)
  pkill -f airflow
  ;;

batches)
  copy_batches
  ;;

init)
  create_venv
  export_airflow_env
  init_airflow
  ;;

venv)
  create_venv
  ;;

clean)
  remove_trash
  rm -rf "${SCRIPT_DIR}/venv"
  echo "Virtual environment had been deleted!"
  ;;

rm-trash)
  remove_trash
  ;;

*)
  show_help
  ;;

esac
