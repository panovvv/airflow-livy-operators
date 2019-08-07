"""
This folder gets mounted as a DAG folder - Airflow sources its DAGs from it.
Instead of DAGs, here we have a script that selectively loads DAGS depending on
the environment where it's run.
"""
