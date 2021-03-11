"""
Sessions are okay unless you have to write more than one-liners.
That's where things get ugly - they can't be declared in code as string literals due to
their size, so we store them in a file. Jinja template placeholders in the code
make it real hard to run, debug and test.
"""

import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from jinja2 import Template

try:
    from airflow_livy.session import LivySessionOperator
except ImportError:
    from airflow_home.plugins.airflow_livy.session import LivySessionOperator


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
    description="Run Spark job via Livy Sessions, load statement from file",
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
        "file2_infer_schema": "false",
        "file2_schema": "`Last name` STRING, `First name` STRING, SSN STRING, "
        "Address1 STRING, Address2 STRING",
        "file2_header": "false",
        "file2_join_column": "SSN",
        "output_columns": "file1.`Last name`, file1.`First name`, file1.SSN, "
        "file2.Address1, file2.Address2",
        # uncomment
        # "output_path": "file:///data/output/session_example_load_from_file/"
        #                "{{ run_id|replace(':', '-') }}"
        # to save result to a file
        "output_sep": "\\t",
        "output_header": "true",
        "output_mode": "overwrite",
    },
    dag=dag,
)

# In first example, we specified code language per statement.
# Instead, you can specify a session-wide language,
# and then override it on per-statement basis.
code = "{{ task_instance.xcom_pull(key='join_code', task_ids='get_session_code') }}"
run_session = LivySessionOperator(
    name="livy_session_example_load_from_file_{{ run_id }}",
    kind="pyspark",
    statements=[
        LivySessionOperator.Statement(code=code),
        LivySessionOperator.Statement(
            kind="sql", code="SELECT 'Hello world! I am a SQL code in Pyspark session!'"
        ),
    ],
    task_id="livy_session_example_load_from_file",
    dag=dag,
)

run_session << get_session_code
