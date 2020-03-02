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

export_airflow_env() {
  AIRFLOW_HOME="${SCRIPT_DIR}/airflow_home"
  export AIRFLOW_HOME
  export AIRFLOW__CORE__LOAD_EXAMPLES=False
  export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8888
  export SLUGIFY_USES_TEXT_UNIDECODE=yes
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
    -exec rm -rv {} +
  find "${SCRIPT_DIR}" -path "${SCRIPT_DIR}/venv" -prune -o -type d \
    \( -name 'metastore_db' -or -name '.pytest_cache' -or -name '.tox' \
    -or -name 'htmlcov' -or -name '*.egg-info' \
    -or -name 'build' -or -name 'dist' \) -exec rm -rv {} +
  find "${SCRIPT_DIR}" -path "${SCRIPT_DIR}/venv" -prune -o \
    \( -name '*.pyc' -or -name '*.pyo' -or -name '*~' -or -name 'coverage.xml' \
    -or -name '__pycache__' -or -name 'derby.log' -or -name '.coverage' \) \
    -exec rm -rv {} +
  echo "All of the extra files had been deleted!"
}

show_help() {
  echo "Options for just running the examples:"
  echo "up         -   bring up Airflow at http://localhost:8888/admin/ to run test DAGs"
  echo "down       -   tear down Airflow"
  echo
  echo "Development options:"
  echo "cov        -   run tests and generate HTML coverage report."
  echo "lint       -   see what's wrong with the code style"
  echo "format     -   reformat code"
  echo "ci         -   run the same commands that CI runs"
  echo "pypi       -   prepare the package for PyPi."
  echo "               after that you just run"
  echo "    twine upload --repository-url https://test.pypi.org/legacy/ dist/*"
  echo "               for Test PyPi, or this for PyPi:"
  echo "    twine upload dist/*"
  echo "clean      -   previous + delete virtual environment, i.e. full cleaning."
  echo "rm-trash   -   remove Python and Airflow debris"
  echo
  echo "Rarely used separately, supplementary options:"
  echo "venv       -   install virtual environment with all requirements for Airflow and PySpark"
  echo "dev        -   prepare development environment."
  echo "init       -   prep Airflow database, variables, connections etc."
  echo "batches    -   copy the batches from batches/ to where Spark cluster can reach them.)"
  echo "               Re-define as needed (e.g. aws s3 cp batches/ s3:/dev/pyspark)"
}

case "$1" in

# Options for just running the examples:"

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

# Development options:

cov)
  create_venv
  init_dev
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  pytest --cov=airflow_home.plugins --cov-report=html
  deactivate
  ;;

lint)
  create_venv
  init_dev
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  flake8
  deactivate
  ;;

format)
  create_venv
  init_dev
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  black airflow_home/ batches/ tests/
  isort -rc airflow_home/ batches/ tests/
  deactivate
  ;;

ci)
  create_venv
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  if ! python -c "import tox" > /dev/null 2>&1
  then
    pip install tox==3.14.5
  fi
  tox
  deactivate
  ;;

pypi)
  create_venv
  . "${SCRIPT_DIR}/venv/bin/activate"
  cd "${SCRIPT_DIR}" || exit
  if ! python -c "import twine" > /dev/null 2>&1
  then
    pip install twine==3.1.1
  fi
  remove_trash
  python setup.py bdist_wheel
  python setup.py sdist
  twine check dist/*
  deactivate
  ;;

clean)
  remove_trash
  rm -rf "${SCRIPT_DIR}/venv"
  echo "Virtual environment had been deleted!"
  ;;

rm-trash)
  remove_trash
  ;;

# Rarely used separately, supplementary options:

venv)
  create_venv
  ;;

dev)
  create_venv
  init_dev
  ;;

init)
  create_venv
  export_airflow_env
  init_airflow
  ;;

batches)
  copy_batches
  ;;

*)
  show_help
  ;;

esac
