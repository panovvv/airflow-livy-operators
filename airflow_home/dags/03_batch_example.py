"""
Solution to the problem of running "serious" code in sessions is Livy Batches.

Batches work the same way running ordinary code works:
a) you can pass arguments into your code (no need for templates anymore).
b) you can run and debug batch files even locally in IDE!
"""

from datetime import datetime

from airflow import DAG

try:
    from airflow_livy.batch import LivyBatchOperator
except ImportError:
    from airflow_home.plugins.airflow_livy.session import LivyBatchOperator

dag = DAG(
    "03_batch_example",
    description="Run Spark job via Livy Batches, verify status in Livy as well",
    schedule_interval=None,
    start_date=datetime(1970, 1, 1),
    catchup=False,
)

# name and arguments parameters can still be templated (see below),
# but batch code is template-free (see /batches/join_2_files.py)
op = LivyBatchOperator(
    name="batch_example_{{ run_id }}",
    file="file:///data/batches/join_2_files.py",
    py_files=["file:///data/batches/join_2_files.py"],
    # Required arguments are positional, meaning you don't have to specify their name.
    # In this case, they are file1_path and file2_path.
    arguments=[
        "file:///data/grades.csv",
        "file:///data/ssn-address.tsv",
        "-file1_sep=,",
        "-file1_header=true",
        "-file1_schema=`Last name` STRING, `First name` STRING, SSN STRING, "
        "Test1 INT, Test2 INT, Test3 INT, Test4 INT, Final INT, Grade STRING",
        "-file1_join_column=SSN",
        "-file2_header=false",
        "-file2_schema=`Last name` STRING, `First name` STRING, SSN STRING, "
        "Address1 STRING, Address2 STRING",
        "-file2_join_column=SSN",
        # uncomment
        # "-output_path=file:///data/output/livy_batch_example/"
        # "{{ run_id|replace(':', '-') }}",
        # to save result to a file
        "-output_header=true",
        "-output_columns=file1.`Last name`, file1.`First name`, file1.SSN, "
        "file2.Address1, file2.Address2",
    ],
    task_id="livy_batch_example",
    dag=dag,
)
