"""
Sessions are okay unless you have to write more than one-liners.
That's where things get ugly - they can't be included as string literals due to
their size, so we store them in a file. Jinja templates make it hard/impossible
to run, debug and test it.
"""
import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from jinja2 import Template

try:
    # Import statement for Airflow when it loads new operators into airflow.operators
    from airflow.operators import LivySessionOperator
except ImportError:
    # Import statement for IDE with the local folder structure
    from airflow_home.plugins.livy_session_plugin import LivySessionOperator


def read_code_from_file(task_instance, **context):
    session_files_path = Variable.get("session_files_path")
    join_code_path = os.path.join(session_files_path, "join_2_files.py")
    logging.info(f"Reading the session code file from {join_code_path}")
    with open(join_code_path) as join_code_file:
        join_code = join_code_file.read()
    template = Template(join_code)
    rendered_code = template.render(context)
    task_instance.xcom_push(key="join_code", value=rendered_code)
    return "\n" + rendered_code


dag = DAG(
    "02_session_example_load_from_file",
    description="Running Spark jobs via Livy Sessions, loading statement from file",
    schedule_interval=None,
    start_date=datetime(1970, 1, 1),
    catchup=False,
)

get_session_code = PythonOperator(
    task_id="get_session_code",
    provide_context=True,
    python_callable=read_code_from_file,
    op_kwargs={
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
        "output_path": "file:///data/output/livy_session_example_load_from_file",
        "output_sep": "\\t",
        "output_header": "true",
        "output_mode": "overwrite",
    },
    dag=dag,
)

run_session = LivySessionOperator(
    name="livy_session_example_load_from_file_{{ run_id }}",
    statements=[
        LivySessionOperator.Statement(
            code="{{ task_instance.xcom_pull(key='join_code', task_ids='get_session_code') }}",
            kind="pyspark",
        )
    ],
    task_id="livy_session_example_load_from_file",
    dag=dag,
)

run_session << get_session_code
