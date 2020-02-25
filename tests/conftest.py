from datetime import datetime

from airflow import DAG
from pytest import fixture


@fixture(scope="session", autouse=True)
def welcome():
    print("\n\nPytest session starting...\n")
    yield
    print("\n\nPytest session is over")


@fixture(scope="session")
def dag():
    yield DAG("test_dag", start_date=datetime(1970, 1, 1))
