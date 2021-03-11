"""
This is the same failing batch as 04_batch_example_failing.py,
but we additionally validate batch status via Spark REST API.
"""

from datetime import datetime

from airflow import DAG

try:
    from airflow_livy.batch import LivyBatchOperator
except ImportError:
    from airflow_home.plugins.airflow_livy.session import LivyBatchOperator

dag = DAG(
    "05_batch_example_verify_in_spark",
    description="Run Spark job via Livy Batches, verify status in Spark REST API",
    schedule_interval=None,
    start_date=datetime(1970, 1, 1),
    catchup=False,
)

op = LivyBatchOperator(
    name="batch_example_verify_in_spark_{{ run_id }}",
    file="file:///data/batches/join_2_files.py",
    py_files=["file:///data/batches/join_2_files.py"],
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
        "-output_header=true",
        # The job is supposed to show as "Failed" b/c of that inexistent column
        "-output_columns=file1.`Last name`, file1.Inexistent",
    ],
    verify_in="spark",
    task_id="livy_batch_example_verify_in_spark",
    dag=dag,
)
