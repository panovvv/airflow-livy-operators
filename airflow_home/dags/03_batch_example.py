from datetime import datetime

from airflow import DAG

try:
    # Import statement for Airflow when it loads new operators into airflow.operators
    from airflow.operators import LivyBatchOperator
except ImportError:
    # Import statement for IDE with the local folder structure
    from airflow_home.plugins.livy_batch_plugin import LivyBatchOperator

dag = DAG(
    "03_batch_example",
    description="Running Spark jobs via Livy Batches",
    schedule_interval=None,
    start_date=datetime(1970, 1, 1),
    catchup=False,
)

t1 = LivyBatchOperator(
    name="batch_example_{{ run_id }}",
    file="file:///data/batches/join_2_files.py",
    py_files=["file:///data/batches/join_2_files.py"],
    arguments=[
        "file:///data/grades.csv",
        "file:///data/ssn-address.tsv",
        "file:///data/output/livy_batch_example/",
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
        "-output_columns=file1.`Last name`, file1.`First name`, file1.SSN, "
        "file2.Address1, file2.Address2",
    ],
    task_id="livy_batch_example",
    dag=dag,
)
