import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable

try:
    # Import statement for Airflow when it loads new operators into airflow.operators
    from airflow.operators import LivySessionOperator
except ImportError:
    # Import statement for IDE with the local folder structure
    from airflow_home.plugins.livy_session_plugin import LivySessionOperator

dag = DAG(
    "session_example_load_from_file",
    description="Running Spark jobs via Livy Sessions, loading statenent from file",
    schedule_interval=None,
    start_date=datetime(1970, 1, 1),
    catchup=False,
)

pyspark_path = Variable.get("pyspark_path")
pyspark_code_path = os.path.join(pyspark_path, "session_join_2_files.py")
with open(pyspark_code_path) as code_file:
    pyspark_code = code_file.read()

t1 = LivySessionOperator(
    statements=[LivySessionOperator.Statement(code=pyspark_code, kind="pyspark")],
    name="livy_session_example_load_from_file_{{ run_id }}",
    params={
        "file1_path": "file:///data/grades.csv",
        "file1_sep": ",",
        "file1_infer_schema": "true",
        "file1_header": "true",
        "file1_quote": '\\"',
        "file1_escape": "\\\\",
        "file1_join_column": "SSN",
        "file2_path": "file:///data/ssn-address.tsv",
        "file2_sep": "\\t",
        "file2_infer_schema": "false",
        "file2_schema": "`Last name` STRING, `First name` STRING, SSN STRING, "
        "Address1 STRING, Address2 STRING",
        "file2_header": "false",
        "file2_quote": '\\"',
        "file2_escape": "\\\\",
        "file2_join_column": "SSN",
        "output_columns": "file1.`Last name`, file1.`First name`, file1.SSN, "
        "file2.Address1, file2.Address2",
        "output_path_base": "file:///data/livy_session_example_load_from_file",
        "output_sep": "\\t",
        "output_header": "true",
        "output_mode": "overwrite",
    },
    task_id="livy_session_example_load_from_file",
    dag=dag,
)
