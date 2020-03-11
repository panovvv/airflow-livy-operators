#!/usr/bin/env bash

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd -P)

_echo_color() {
  no_col=$'\033[0m'
  case $1 in
  red) col=$'\e[1;31m' ;;
  green) col=$'\e[1;32m' ;;
  yellow) col=$'\e[1;33m' ;;
  blue) col=$'\e[1;34m' ;;
  magenta) col=$'\e[1;35m' ;;
  cyan) col=$'\e[1;36m' ;;
  esac
  echo "${col}${2}${no_col}"
}

minimal_venv() {
  if [ ! -e "${SCRIPT_DIR}/venv/bin/activate" ]; then
    _echo_color yellow "Creating virtual environment for the first time..."
    python3 -m venv "${SCRIPT_DIR}/venv"
    . "${SCRIPT_DIR}/venv/bin/activate"
    python3 -m pip install --upgrade pip
    python3 -m pip install -r "${SCRIPT_DIR}/requirements.txt"
    cp "${SCRIPT_DIR}/requirements.txt" "${SCRIPT_DIR}/venv/requirements.txt"
    deactivate
  else
    _echo_color green "Virtual environment had already been created."
    if [ -f "${SCRIPT_DIR}/venv/requirements.txt" ]; then
      if diff "${SCRIPT_DIR}/requirements.txt" "${SCRIPT_DIR}/venv/requirements.txt"; then
        _echo_color green "requirements.txt did not change since we last installed it"
      else
        _echo_color yellow "requirements.txt changed since we last installed it"
        . "${SCRIPT_DIR}/venv/bin/activate"
        python3 -m pip install -r "${SCRIPT_DIR}/requirements.txt"
        cp "${SCRIPT_DIR}/requirements.txt" "${SCRIPT_DIR}/venv/requirements.txt"
        deactivate
      fi
    else
      _echo_color red "requirements.txt does not exist in venv folder"
      cp "${SCRIPT_DIR}/requirements.txt" "${SCRIPT_DIR}/venv/requirements.txt"
    fi
  fi
}

development_venv() {
  minimal_venv
  if [ -f "${SCRIPT_DIR}/venv/requirements_dev.txt" ]; then
    if diff "${SCRIPT_DIR}/requirements_dev.txt" "${SCRIPT_DIR}/venv/requirements_dev.txt"; then
      _echo_color green "requirements_dev.txt did not change since we last installed it"
    else
      _echo_color yellow "requirements_dev.txt changed since we last installed it"
      . "${SCRIPT_DIR}/venv/bin/activate"
      python3 -m pip install -r "${SCRIPT_DIR}/requirements_dev.txt"
      cp "${SCRIPT_DIR}/requirements_dev.txt" "${SCRIPT_DIR}/venv/requirements_dev.txt"
      deactivate
    fi
  else
    _echo_color yellow "Installing dev dependencies for the first time..."
    . "${SCRIPT_DIR}/venv/bin/activate"
    python3 -m pip install -r "${SCRIPT_DIR}/requirements_dev.txt"
    cp "${SCRIPT_DIR}/requirements_dev.txt" "${SCRIPT_DIR}/venv/requirements_dev.txt"
    deactivate
  fi
}

export_airflow_env_vars() {
  AIRFLOW_HOME="${SCRIPT_DIR}/airflow_home"
  export AIRFLOW_HOME
  export AIRFLOW__CORE__LOAD_EXAMPLES=False
  export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8888
  export SLUGIFY_USES_TEXT_UNIDECODE=yes
  _echo_color green "Exported all Airflow vars!"
}

init_airflow() {
  if [ ! -e "${SCRIPT_DIR}/airflow_home/airflow.db" ]; then
    . "${SCRIPT_DIR}/venv/bin/activate"
    _echo_color yellow "Initializing Airflow..."
    airflow initdb
    airflow variables -s session_files_path "${SCRIPT_DIR}/sessions"
    airflow variables -s batch_files_path "${SCRIPT_DIR}/batches"
    airflow connections -a --conn_id livy --conn_type HTTP \
      --conn_host localhost --conn_schema http --conn_port 8998
    airflow connections -a --conn_id spark --conn_type HTTP \
      --conn_host localhost --conn_schema http --conn_port 18080
    airflow connections -a --conn_id yarn --conn_type HTTP \
      --conn_host localhost --conn_schema http --conn_port 8088
    airflow unpause 01_session_example &
    airflow unpause 02_session_example_load_from_file &
    airflow unpause 03_batch_example &
    airflow unpause 04_batch_example_failing &
    airflow unpause 05_batch_example_verify_in_spark &
    airflow unpause 06_batch_example_verify_in_yarn &
    deactivate
    _echo_color green "Airflow initialized."
  else
    _echo_color green "Airflow had already been initialized."
  fi
}

copy_batches() {
  TO_DIR="$HOME/data/vpanov/bigdata-docker-compose/data/batches/"
  _echo_color yellow "Copying the batch files from ${SCRIPT_DIR}/batches/ to ${TO_DIR}"
  rsync -arv "${SCRIPT_DIR}"/batches/*.py "${TO_DIR}"
  _echo_color green "Copied the batch files to ${TO_DIR}"
}

remove_trash() {
  _echo_color magenta "Cleaning up..."
  find "${SCRIPT_DIR}/airflow_home" -mindepth 1 -maxdepth 1 \
    ! \( -name 'dags' -or -name 'plugins' -or -name '__init__.py' \) \
    -exec rm -rvf {} +
  find "${SCRIPT_DIR}" -path "${SCRIPT_DIR}/venv" -prune -o -type d \
    \( -name 'metastore_db' -or -name '.pytest_cache' -or -name '.tox' \
    -or -name 'htmlcov' -or -name '*.egg-info' \
    -or -name 'build' -or -name 'dist' \) -exec rm -rvf {} +
  find "${SCRIPT_DIR}" -path "${SCRIPT_DIR}/venv" -prune -o \
    \( -name '*.pyc' -or -name '*.pyo' -or -name '*~' -or -name 'coverage.xml' \
    -or -name '__pycache__' -or -name 'derby.log' -or -name '.coverage' \) \
    -exec rm -rvf {} +
  _echo_color green "All of the extra files had been deleted!"
}

show_help() {
  echo "Options for just running the examples:"
  echo "up         -   bring up Airflow at http://localhost:8888/admin/ to run test DAGs."
  echo "               Plugins are loaded from PyPi"
  echo "down       -   tear down Airflow"
  echo
  echo "Development options:"
  echo "updev      -   same as 'up', but plugins are loaded from the local plugins/ directory"
  echo "cov        -   run tests and generate HTML coverage report. Same command is ran by CI."
  echo "lint       -   see if anything's wrong with the code style. Same command is ran by CI."
  echo "format     -   reformat code"
  echo "pypi       -   prepare the package for PyPi."
  echo "               after that you just run"
  echo "    twine upload --repository-url https://test.pypi.org/legacy/ dist/*"
  echo "               for Test PyPi, or this for PyPi:"
  echo "    twine upload dist/*"
  echo "rm-trash   -   remove Python and Airflow debris"
  echo "clean      -   previous + delete virtual environment, i.e. full cleaning."
  echo
  echo "Rarely used separately, supplementary options:"
  echo "venv       -   install virtual environment with all requirements for Airflow and PySpark."
  echo "dev-venv   -   prepare virtual environment for development."
  echo "batches    -   copy the batches from batches/ to where Spark cluster can reach them."
  echo "               Re-define as needed (e.g. aws s3 cp batches/ s3:/dev/pyspark)"
}

case "$1" in

# Options for just running the examples:

up)
  copy_batches
  minimal_venv
  export_airflow_env_vars
  init_airflow
  . "${SCRIPT_DIR}/venv/bin/activate"
  pip install airflow-livy-operators==0.3
  airflow variables -s load_operators_from "pypi"
  _echo_color cyan "Running Airflow..."
  airflow scheduler &
  airflow webserver
  deactivate
  ;;

down)
  _echo_color magenta "Killing all Airflow processes..."
  pkill -f airflow || true
  _echo_color green "Done!"
  ;;

# Development options:

updev)
  copy_batches
  development_venv
  export_airflow_env_vars
  init_airflow
  . "${SCRIPT_DIR}/venv/bin/activate"
  airflow variables -s load_operators_from "local"
  _echo_color cyan "Running Airflow in development mode..."
  airflow scheduler &
  airflow webserver
  deactivate
  ;;

format)
  development_venv
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  _echo_color blue "Formatting Python files..."
  black airflow_home/ batches/ tests/
  _echo_color blue "Sorting imports..."
  isort -rc airflow_home/ batches/ tests/
  _echo_color green "Done!"
  deactivate
  ;;

cov)
  development_venv
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  _echo_color blue "Generating Pytest coverage report..."
  pytest --cov=airflow_home.plugins.airflow_livy --cov-report=html
  _echo_color green "Done! You'll find the report in htmlcov/"
  deactivate
  ;;

lint)
  development_venv
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  _echo_color blue "Running flake8 on code..."
  flake8
  _echo_color green "Done!"
  deactivate
  ;;

pypi)
  development_venv
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  remove_trash
  python setup.py bdist_wheel
  python setup.py sdist
  twine check dist/*
  deactivate
  ;;

clean)
  remove_trash
  _echo_color magenta "Deleting virtual environment..."
  rm -rf "${SCRIPT_DIR}/venv"
  _echo_color green "Virtual environment had been deleted!"
  ;;

rm-trash)
  remove_trash
  ;;

# Rarely used separately, supplementary options:

venv)
  minimal_venv
  ;;

dev-venv)
  development_venv
  ;;

batches)
  copy_batches
  ;;

*)
  show_help
  ;;

esac
