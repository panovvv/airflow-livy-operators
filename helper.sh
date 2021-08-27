#!/usr/bin/env bash

set -e

# Change this to your locally-accessible directory where you want to copy batches
# for the cluster to see and use them.
# I'm using this to mount them to my docker-compose based cluster.
BATCH_DIR="$HOME/personal/bigdata-docker-compose/data/batches/"

copy_batches() {
  _echo_color yellow "Copying the batch files from ${SCRIPT_DIR}/batches/ to ${BATCH_DIR}"
  rsync -arv "${SCRIPT_DIR}"/batches/*.py "${BATCH_DIR}"
  _echo_color green "Copied the batch files to ${BATCH_DIR}"
}

# You should not need to touch anything below this line. It just werks 8)

show_help() {
  echo "Options for just running the examples:"
  echo "up           -   bring up Airflow at http://localhost:8888 to run test DAGs."
  echo "               Plugins are loaded from PyPi"
  echo "down         -   tear down Airflow"
  echo
  echo "Development options:"
  echo "updev        -   same as 'up', but plugins are loaded from the local plugins/ directory"
  echo "ci           -   run all CI checks locally: cov, lint, check-format"
  echo "full         -   prepare and check the code: cov, lint, format"
  echo "pypi         -   prepare the package for PyPi."
  echo "                 after that you just run"
  echo "    twine upload --repository-url https://test.pypi.org/legacy/ dist/*"
  echo "                 for Test PyPi, or this for PyPi:"
  echo "    twine upload dist/*"
  echo "                 Alternatively, TravisCI pushes to PyPi on each successful version git tag."
  echo "rm-trash     -   remove Python and Airflow debris"
  echo "clean        -   previous + delete virtual environment, i.e. full cleaning."
  echo
  echo "Rarely used separately, supplementary options:"
  echo "venv         -   install virtual environment with all requirements for Airflow and PySpark."
  echo "dev-venv     -   prepare virtual environment for development."
  echo "batches      -   copy the batches from batches/ to where Spark cluster can reach them."
  echo "                 Re-define if needed (e.g. aws s3 cp batches/ s3:/dev/pyspark)"
  echo "cov          -   run tests and generate HTML coverage report (pytest)."
  echo "lint         -   see if anything's wrong with the code style (flake8)."
  echo "format       -   reformat code (black + isort)"
  echo "check-format -   check code formatting (black + isort)"
}

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
    airflow db init
    airflow users create -u admin -p admin \
      -e email -f first -l last -r Admin
    airflow variables set session_files_path "${SCRIPT_DIR}/sessions"
    airflow variables set batch_files_path "${SCRIPT_DIR}/batches"
    airflow connections add livy --conn-type HTTP \
      --conn-host localhost --conn-schema http --conn-port 8998
    airflow connections add spark --conn-type HTTP \
      --conn-host localhost --conn-schema http --conn-port 18080
    airflow connections add yarn --conn-type HTTP \
      --conn-host localhost --conn-schema http --conn-port 8088
    airflow dags unpause 01_session_example &
    airflow dags unpause 02_session_example_load_from_file &
    airflow dags unpause 03_batch_example &
    airflow dags unpause 04_batch_example_failing &
    airflow dags unpause 05_batch_example_verify_in_spark &
    airflow dags unpause 06_batch_example_verify_in_yarn &
    deactivate
    _echo_color green "Airflow initialized."
  else
    _echo_color green "Airflow had already been initialized."
  fi
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

run_pytest() {
  development_venv
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  _echo_color blue "Generating Pytest coverage report..."
  pytest --cov=airflow_home.plugins.airflow_livy --cov-report=html
  _echo_color green "Done! You'll find the report in htmlcov/"
  deactivate
}

run_flake8() {
  development_venv
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  _echo_color blue "Running flake8 on code..."
  flake8
  _echo_color green "Done!"
  deactivate
}

run_formatting() {
  development_venv
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  _echo_color blue "Formatting Python files with black..."
  black airflow_home/ batches/ tests/
  _echo_color blue "Sorting imports with isort..."
  isort airflow_home/ batches/ tests/
  _echo_color green "Done!"
  deactivate
}

check_formatting() {
  development_venv
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  _echo_color blue "Checking formatting with black..."
  black --check airflow_home/ batches/ tests/
  _echo_color blue "Checking sorting with isort..."
  isort --check-only airflow_home/ batches/ tests/
  _echo_color green "Done!"
  deactivate
}

case "$1" in

# Options for just running the examples:

up)
  copy_batches
  minimal_venv
  export_airflow_env_vars
  init_airflow
  . "${SCRIPT_DIR}/venv/bin/activate"
  pip install airflow-livy-operators==0.6
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
  _echo_color cyan "Running Airflow in development mode..."
  airflow scheduler &
  airflow webserver
  deactivate
  ;;

ci)
  check_formatting
  run_flake8
  run_pytest
  ;;

full)
  run_formatting
  run_flake8
  run_pytest
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

cov)
  run_pytest
  ;;

lint)
  run_flake8
  ;;

format)
  run_formatting
  ;;

check-format)
  check_formatting
  ;;

*)
  show_help
  ;;

esac

set +e
