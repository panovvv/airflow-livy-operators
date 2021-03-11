"""
This is the DAG that will show you one interesting specific of how Livy works
in local or YARN client mode vs YARN cluster mode.

This DAG runs an intentionally failing Livy batch - if your cluster supports
aforementioned configuration, in some cases you'll see the batch fail with
Livy reporting successful job.
"""

from datetime import datetime

from airflow import DAG

try:
    from airflow_livy.batch import LivyBatchOperator
except ImportError:
    from airflow_home.plugins.airflow_livy.session import LivyBatchOperator

dag = DAG(
    "04_batch_example_failing",
    description="Run Spark job via Livy Batches, verify status in Livy as well. "
    "Intentionally failing the job",
    schedule_interval=None,
    start_date=datetime(1970, 1, 1),
    catchup=False,
)

op = LivyBatchOperator(
    name="batch_example_failing_{{ run_id }}",
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
    # This will work if deploy mode is not specified in livy.conf already.
    # It's 'client' by default
    conf={"spark.submit.deployMode": "cluster"},
    task_id="livy_batch_example_failing",
    dag=dag,
)
