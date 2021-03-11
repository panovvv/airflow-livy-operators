"""
Livy can run Spark code in two modes: sessions and batches.

Sessions are good for short, ad-hoc code snippets.
There's no way to pass parameters into statements natively (i.e. via REST API)
so we're making our code templated and render it with Jinja.
params dict contains variables to plug into code template.
"""

from datetime import datetime

from airflow import DAG

try:
    # Runtime import, local code or PyPi
    from airflow_livy.session import LivySessionOperator
except ImportError:
    # Static import so that IDE sees it, also local code.
    from airflow_home.plugins.airflow_livy.session import LivySessionOperator

dag = DAG(
    "01_session_example",
    description="Run Spark job via Livy Sessions",
    schedule_interval=None,
    start_date=datetime(1970, 1, 1),
    catchup=False,
)

# See ready statements with parameter values substituted
# in the "Rendered template" tab of a running task.
scala_code = """
spark.range(1000 * 1000 * {{ params.your_number }}).count()
val df = Seq(
  ("This was Scala code", 0),
  ("One", 1),
  ("Two", 2),
  ("Three", 3),
  ("Four", 4)
).toDF("{{ params.your_string }}", "{{ run_id }}")
df.show()
"""

pyspark_code = """
import sys

print(sys.version)
spark.range(1000 * 1000 * {{params.your_number}}).count()
df = sqlContext.createDataFrame(
    [("This was Python code", 0), ("One", 1), ("Two", 2), ("Three", 3), ("Four", 4)],
    ("{{ params.your_string }}", "{{ run_id }}"),
)
df.show()
"""

sparkr_code = """
df <- as.DataFrame(
    list("{{ params.your_number }}", "{{ run_id }}", "Three", "Four",
    "This was R code"), "{{ params.your_string }}")
head(df)
"""

sql_code = """
SELECT CONCAT(
  '{{ params.your_string }}',
  ' in task instance ',
  '{{ run_id }}',
  ' - this was SQL query'
)
"""

# See the results of each statement's executions under "Logs" tab of the task.
op = LivySessionOperator(
    name="livy_session_example_{{ run_id }}",
    statements=[
        LivySessionOperator.Statement(code=scala_code, kind="spark"),
        LivySessionOperator.Statement(code=pyspark_code, kind="pyspark"),
        LivySessionOperator.Statement(code=sparkr_code, kind="sparkr"),
        LivySessionOperator.Statement(code=sql_code, kind="sql"),
    ],
    params={"your_number": 5, "your_string": "Hello world"},
    task_id="livy_session_example",
    dag=dag,
)
